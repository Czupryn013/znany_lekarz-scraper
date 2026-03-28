"""In-memory bipartite graph of clinic-doctor connections with pathfinding."""

import heapq
import logging
from collections import defaultdict, deque

from sqlalchemy.orm import Session
from sqlalchemy import text

logger = logging.getLogger(__name__)

MAX_NEIGHBORHOOD_NODES = 500


def load_graph(session: Session) -> tuple[dict[int, set[int]], dict[int, set[int]]]:
    """Load the full clinic-doctor graph from DB into adjacency dicts."""
    logger.info("Loading clinic-doctor graph from database...")
    rows = session.execute(text("SELECT clinic_id, doctor_id FROM clinic_doctors")).fetchall()

    clinic_to_doctors: dict[int, set[int]] = defaultdict(set)
    doctor_to_clinics: dict[int, set[int]] = defaultdict(set)

    for clinic_id, doctor_id in rows:
        clinic_to_doctors[clinic_id].add(doctor_id)
        doctor_to_clinics[doctor_id].add(clinic_id)

    logger.info(
        "Graph loaded: %d clinics, %d doctors, %d edges",
        len(clinic_to_doctors),
        len(doctor_to_clinics),
        len(rows),
    )
    return dict(clinic_to_doctors), dict(doctor_to_clinics)


def load_doctor_specs(session: Session) -> dict[int, set[int]]:
    """Load doctor_id -> set of specialization_ids for in-memory spec search."""
    logger.info("Loading doctor-specialization map...")
    rows = session.execute(text("SELECT doctor_id, specialization_id FROM doctor_specializations")).fetchall()

    result: dict[int, set[int]] = defaultdict(set)
    for doc_id, spec_id in rows:
        result[doc_id].add(spec_id)

    logger.info("Doctor specs loaded: %d doctors with specializations", len(result))
    return dict(result)


def load_nip_mapping(session: Session) -> tuple[dict[int, int], dict[int, list[int]]]:
    """Load NIP-based clinic grouping. Returns (clinic_id -> rep_id, rep_id -> [member_ids])."""
    logger.info("Loading NIP-based clinic groups...")
    rows = session.execute(
        text("SELECT id, nip, doctors_count FROM clinics WHERE nip IS NOT NULL AND nip != ''")
    ).fetchall()

    nip_to_clinics: dict[str, list[tuple[int, int]]] = defaultdict(list)
    for cid, nip, doc_count in rows:
        nip_to_clinics[nip].append((cid, doc_count or 0))

    clinic_to_rep: dict[int, int] = {}
    rep_to_members: dict[int, list[int]] = {}

    for nip, clinics in nip_to_clinics.items():
        if len(clinics) < 2:
            continue
        # pick the clinic with most doctors as representative
        clinics.sort(key=lambda x: x[1], reverse=True)
        rep_id = clinics[0][0]
        member_ids = [c[0] for c in clinics]
        rep_to_members[rep_id] = member_ids
        for cid, _ in clinics:
            clinic_to_rep[cid] = rep_id

    logger.info(
        "NIP groups: %d groups covering %d clinics",
        len(rep_to_members),
        len(clinic_to_rep),
    )
    return clinic_to_rep, rep_to_members


def build_merged_graph(
    c2d: dict[int, set[int]],
    d2c: dict[int, set[int]],
    clinic_to_rep: dict[int, int],
) -> tuple[dict[int, set[int]], dict[int, set[int]]]:
    """Build a merged graph where clinics sharing a NIP collapse into one node."""
    merged_c2d: dict[int, set[int]] = defaultdict(set)
    merged_d2c: dict[int, set[int]] = defaultdict(set)

    for clinic_id, doctors in c2d.items():
        rep_id = clinic_to_rep.get(clinic_id, clinic_id)
        merged_c2d[rep_id].update(doctors)
        for doc_id in doctors:
            merged_d2c[doc_id].add(rep_id)

    logger.info(
        "Merged graph: %d clinics (was %d), %d doctors",
        len(merged_c2d), len(c2d), len(merged_d2c),
    )
    return dict(merged_c2d), dict(merged_d2c)


def get_neighborhood(
    c2d: dict[int, set[int]],
    d2c: dict[int, set[int]],
    node_type: str,
    node_id: int,
    depth: int = 1,
) -> dict:
    """BFS from a node up to `depth` hops, returning nodes and edges."""
    nodes: list[dict] = []
    edges: list[dict] = []
    visited: set[tuple[str, int]] = set()
    queue: deque[tuple[str, int, int]] = deque()

    queue.append((node_type, node_id, 0))
    visited.add((node_type, node_id))
    truncated = False

    while queue:
        ntype, nid, d = queue.popleft()
        nodes.append({"id": f"{'c' if ntype == 'clinic' else 'd'}_{nid}", "type": ntype, "raw_id": nid})

        if len(nodes) >= MAX_NEIGHBORHOOD_NODES:
            truncated = True
            break

        if d >= depth:
            continue

        if ntype == "clinic":
            for doc_id in c2d.get(nid, set()):
                edges.append({"source": f"c_{nid}", "target": f"d_{doc_id}"})
                if ("doctor", doc_id) not in visited:
                    visited.add(("doctor", doc_id))
                    queue.append(("doctor", doc_id, d + 1))
        else:
            for cli_id in d2c.get(nid, set()):
                edges.append({"source": f"d_{nid}", "target": f"c_{cli_id}"})
                if ("clinic", cli_id) not in visited:
                    visited.add(("clinic", cli_id))
                    queue.append(("clinic", cli_id, d + 1))

    return {"nodes": nodes, "edges": edges, "truncated": truncated}


def find_doctors_by_specialization(
    c2d: dict[int, set[int]],
    d2c: dict[int, set[int]],
    start_clinics: list[int],
    target_spec_ids: set[int],
    doctor_specs: dict[int, set[int]],
    max_hops: int = 3,
    max_results: int = 50,
) -> dict:
    """BFS from one or more clinics to find doctors with target specializations within max_hops.

    Phase 1: lightweight BFS on raw IDs only (no node/edge building) — explores full graph.
    Phase 2: trace back paths from matched doctors to start, build subgraph from those paths only.
    """
    # Phase 1: BFS — just visited + parent pointers
    parent: dict[tuple[str, int], tuple[str, int] | None] = {}
    depth_map: dict[tuple[str, int], int] = {}
    queue: deque[tuple[str, int, int]] = deque()

    for sc in start_clinics:
        start = ("clinic", sc)
        queue.append((*start, 0))
        parent[start] = None
        depth_map[start] = 0

    results: list[dict] = []
    found_doctors: set[int] = set()

    while queue and len(results) < max_results:
        ntype, nid, depth = queue.popleft()

        if depth >= max_hops:
            continue

        if ntype == "clinic":
            for doc_id in c2d.get(nid, set()):
                node = ("doctor", doc_id)
                # Check spec match regardless of visited (doctor may be reachable via multiple paths)
                if doc_id not in found_doctors:
                    doc_spec_ids = doctor_specs.get(doc_id, set())
                    matching = doc_spec_ids & target_spec_ids
                    if matching:
                        found_doctors.add(doc_id)
                        results.append({
                            "doctor_id": doc_id,
                            "matching_spec_ids": list(matching),
                            "hops": depth + 1,
                            "via_clinic": nid,
                        })

                if node not in parent:
                    parent[node] = ("clinic", nid)
                    depth_map[node] = depth + 1
                    queue.append(("doctor", doc_id, depth + 1))
        else:
            for cli_id in d2c.get(nid, set()):
                node = ("clinic", cli_id)
                if node not in parent:
                    parent[node] = ("doctor", nid)
                    depth_map[node] = depth + 1
                    queue.append(("clinic", cli_id, depth + 1))

    logger.info(
        "Spec search BFS: visited %d nodes, found %d matching doctors",
        len(parent), len(results),
    )

    # Phase 2: trace paths from each matched doctor back to start, build subgraph
    keep_nodes: set[tuple[str, int]] = set()
    for r in results:
        # Trace from the matched doctor back to start
        cur: tuple[str, int] | None = ("doctor", r["doctor_id"])
        while cur is not None:
            keep_nodes.add(cur)
            cur = parent.get(cur)
        # Also keep the clinic where the doctor was found (for the edge)
        keep_nodes.add(("clinic", r["via_clinic"]))

    # Build nodes and edges for the kept subgraph
    nodes: list[dict] = []
    edges: list[dict] = []
    node_ids: set[str] = set()

    for ntype, nid in keep_nodes:
        sid = f"{'c' if ntype == 'clinic' else 'd'}_{nid}"
        if sid not in node_ids:
            node_ids.add(sid)
            nodes.append({"id": sid, "type": ntype, "raw_id": nid})

    # Add edges between consecutive nodes on the kept paths
    for ntype, nid in keep_nodes:
        sid = f"{'c' if ntype == 'clinic' else 'd'}_{nid}"
        p = parent.get((ntype, nid))
        if p is not None:
            pid = f"{'c' if p[0] == 'clinic' else 'd'}_{p[1]}"
            if pid in node_ids:
                edges.append({"source": pid, "target": sid})

    # For matched doctors, also add edge from their clinic (via_clinic) if not already there
    for r in results:
        doc_sid = f"d_{r['doctor_id']}"
        cli_sid = f"c_{r['via_clinic']}"
        edge_exists = any(
            (e["source"] == cli_sid and e["target"] == doc_sid) or
            (e["source"] == doc_sid and e["target"] == cli_sid)
            for e in edges
        )
        if not edge_exists:
            edges.append({"source": cli_sid, "target": doc_sid})

    # Clean up via_clinic from results (internal detail)
    for r in results:
        del r["via_clinic"]

    return {
        "results": results,
        "nodes": nodes,
        "edges": edges,
        "total_found": len(results),
    }


def _bfs_shortest_path(
    c2d: dict[int, set[int]],
    d2c: dict[int, set[int]],
    start_clinic: int,
    end_clinic: int,
    blocked: set[tuple[str, int]] | None = None,
) -> list[tuple[str, int]] | None:
    """BFS shortest path on bipartite graph from start_clinic to end_clinic."""
    if start_clinic == end_clinic:
        return [("clinic", start_clinic)]

    if blocked is None:
        blocked = set()

    visited: set[tuple[str, int]] = set(blocked)
    parent: dict[tuple[str, int], tuple[str, int] | None] = {}

    start = ("clinic", start_clinic)
    end = ("clinic", end_clinic)

    if start in visited or end in blocked:
        return None

    visited.add(start)
    parent[start] = None
    queue: deque[tuple[str, int]] = deque([start])

    while queue:
        ntype, nid = queue.popleft()

        if ntype == "clinic":
            for doc_id in c2d.get(nid, set()):
                neighbor = ("doctor", doc_id)
                if neighbor not in visited:
                    visited.add(neighbor)
                    parent[neighbor] = (ntype, nid)
                    queue.append(neighbor)
        else:
            for cli_id in d2c.get(nid, set()):
                neighbor = ("clinic", cli_id)
                if neighbor not in visited:
                    visited.add(neighbor)
                    parent[neighbor] = (ntype, nid)
                    if neighbor == end:
                        # reconstruct
                        path = []
                        cur: tuple[str, int] | None = neighbor
                        while cur is not None:
                            path.append(cur)
                            cur = parent[cur]
                        return path[::-1]
                    queue.append(neighbor)

    return None


def _path_similarity(path_a: list[tuple[str, int]], path_b: list[tuple[str, int]]) -> float:
    """Jaccard similarity of intermediate nodes between two paths."""
    if len(path_a) <= 2 and len(path_b) <= 2:
        return 0.0
    inner_a = set(path_a[1:-1])
    inner_b = set(path_b[1:-1])
    if not inner_a and not inner_b:
        return 0.0
    intersection = len(inner_a & inner_b)
    union = len(inner_a | inner_b)
    return intersection / union if union else 0.0


def yen_k_shortest_paths(
    c2d: dict[int, set[int]],
    d2c: dict[int, set[int]],
    start_clinic: int,
    end_clinic: int,
    k: int = 5,
    max_similarity: float = 0.7,
) -> list[list[tuple[str, int]]]:
    """Find k diverse shortest paths between two clinics using Yen's algorithm."""
    logger.info("Finding %d paths from clinic %d to clinic %d", k, start_clinic, end_clinic)

    shortest = _bfs_shortest_path(c2d, d2c, start_clinic, end_clinic)
    if shortest is None:
        logger.info("No path found between clinic %d and %d", start_clinic, end_clinic)
        return []

    accepted: list[list[tuple[str, int]]] = [shortest]
    # candidates: (path_length, path_index_for_tiebreak, path)
    candidates: list[tuple[int, int, list[tuple[str, int]]]] = []
    candidate_counter = 0

    for ki in range(1, k * 3):  # search more candidates to allow diversity filtering
        prev_path = accepted[-1] if accepted else shortest

        for i in range(len(prev_path) - 1):
            spur_node = prev_path[i]
            root_path = prev_path[:i + 1]

            blocked: set[tuple[str, int]] = set()
            # block edges used by existing paths at this spur point
            for path in accepted:
                if path[:i + 1] == root_path and i + 1 < len(path):
                    blocked.add(path[i + 1])

            # also block root path nodes (except spur) to avoid loops
            for node in root_path[:-1]:
                blocked.add(node)

            spur_path = _bfs_shortest_path(c2d, d2c, spur_node[1], end_clinic, blocked)
            if spur_path is not None:
                total_path = root_path[:-1] + spur_path
                heapq.heappush(candidates, (len(total_path), candidate_counter, total_path))
                candidate_counter += 1

        if not candidates:
            break

        # pick next best candidate that is diverse enough
        while candidates:
            _, _, candidate = heapq.heappop(candidates)

            is_diverse = all(
                _path_similarity(candidate, acc) < max_similarity
                for acc in accepted
            )
            if is_diverse:
                accepted.append(candidate)
                break

        if len(accepted) >= k:
            break

    logger.info("Found %d diverse paths", len(accepted))
    return accepted[:k]
