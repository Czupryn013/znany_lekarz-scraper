"""In-memory bipartite graph of lead-clinic connections with pathfinding."""

import heapq
import logging
from collections import defaultdict, deque

from sqlalchemy.orm import Session
from sqlalchemy import text

logger = logging.getLogger(__name__)

MAX_NEIGHBORHOOD_NODES = 500


def load_lead_graph(session: Session) -> tuple[dict[int, set[int]], dict[int, set[int]]]:
    """Load the full lead-clinic graph from DB into adjacency dicts (ICP clinics only)."""
    logger.info("Loading lead-clinic graph from database...")
    rows = session.execute(
        text("""
            SELECT DISTINCT lcr.clinic_id, lcr.lead_id
            FROM lead_clinic_roles lcr
            JOIN clinics c ON c.id = lcr.clinic_id
            WHERE c.icp_match = true
        """)
    ).fetchall()

    clinic_to_leads: dict[int, set[int]] = defaultdict(set)
    lead_to_clinics: dict[int, set[int]] = defaultdict(set)

    for clinic_id, lead_id in rows:
        clinic_to_leads[clinic_id].add(lead_id)
        lead_to_clinics[lead_id].add(clinic_id)

    logger.info(
        "Lead graph loaded: %d clinics, %d leads, %d edges",
        len(clinic_to_leads),
        len(lead_to_clinics),
        len(rows),
    )
    return dict(clinic_to_leads), dict(lead_to_clinics)


def load_lead_roles(session: Session) -> dict[tuple[int, int], list[str]]:
    """Load lead-clinic roles for display. Returns {(lead_id, clinic_id): [roles]}."""
    logger.info("Loading lead-clinic roles...")
    rows = session.execute(
        text("""
            SELECT lcr.lead_id, lcr.clinic_id, lcr.role
            FROM lead_clinic_roles lcr
            JOIN clinics c ON c.id = lcr.clinic_id
            WHERE c.icp_match = true
        """)
    ).fetchall()

    roles: dict[tuple[int, int], list[str]] = defaultdict(list)
    for lead_id, clinic_id, role in rows:
        roles[(lead_id, clinic_id)].append(role)

    return dict(roles)


def load_lead_nip_mapping(session: Session) -> tuple[dict[int, int], dict[int, list[int]]]:
    """Load NIP-based clinic grouping for ICP clinics only."""
    logger.info("Loading NIP-based lead-clinic groups...")
    rows = session.execute(
        text("""
            SELECT id, nip, doctors_count FROM clinics
            WHERE icp_match = true AND nip IS NOT NULL AND nip != ''
        """)
    ).fetchall()

    nip_to_clinics: dict[str, list[tuple[int, int]]] = defaultdict(list)
    for cid, nip, doc_count in rows:
        nip_to_clinics[nip].append((cid, doc_count or 0))

    clinic_to_rep: dict[int, int] = {}
    rep_to_members: dict[int, list[int]] = {}

    for nip, clinics in nip_to_clinics.items():
        if len(clinics) < 2:
            continue
        clinics.sort(key=lambda x: x[1], reverse=True)
        rep_id = clinics[0][0]
        member_ids = [c[0] for c in clinics]
        rep_to_members[rep_id] = member_ids
        for cid, _ in clinics:
            clinic_to_rep[cid] = rep_id

    logger.info(
        "Lead NIP groups: %d groups covering %d clinics",
        len(rep_to_members),
        len(clinic_to_rep),
    )
    return clinic_to_rep, rep_to_members


def build_lead_merged_graph(
    c2l: dict[int, set[int]],
    l2c: dict[int, set[int]],
    clinic_to_rep: dict[int, int],
) -> tuple[dict[int, set[int]], dict[int, set[int]]]:
    """Build a merged lead graph where clinics sharing a NIP collapse into one node."""
    merged_c2l: dict[int, set[int]] = defaultdict(set)
    merged_l2c: dict[int, set[int]] = defaultdict(set)

    for clinic_id, leads in c2l.items():
        rep_id = clinic_to_rep.get(clinic_id, clinic_id)
        merged_c2l[rep_id].update(leads)
        for lead_id in leads:
            merged_l2c[lead_id].add(rep_id)

    logger.info(
        "Merged lead graph: %d clinics (was %d), %d leads",
        len(merged_c2l), len(c2l), len(merged_l2c),
    )
    return dict(merged_c2l), dict(merged_l2c)


def get_lead_neighborhood(
    c2l: dict[int, set[int]],
    l2c: dict[int, set[int]],
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
        nodes.append({"id": f"{'c' if ntype == 'clinic' else 'l'}_{nid}", "type": ntype, "raw_id": nid})

        if len(nodes) >= MAX_NEIGHBORHOOD_NODES:
            truncated = True
            break

        if d >= depth:
            continue

        if ntype == "clinic":
            for lead_id in c2l.get(nid, set()):
                edges.append({"source": f"c_{nid}", "target": f"l_{lead_id}"})
                if ("lead", lead_id) not in visited:
                    visited.add(("lead", lead_id))
                    queue.append(("lead", lead_id, d + 1))
        else:
            for cli_id in l2c.get(nid, set()):
                edges.append({"source": f"l_{nid}", "target": f"c_{cli_id}"})
                if ("clinic", cli_id) not in visited:
                    visited.add(("clinic", cli_id))
                    queue.append(("clinic", cli_id, d + 1))

    return {"nodes": nodes, "edges": edges, "truncated": truncated}


def _bfs_lead_shortest_path(
    c2l: dict[int, set[int]],
    l2c: dict[int, set[int]],
    start_clinic: int,
    end_clinic: int,
    blocked: set[tuple[str, int]] | None = None,
) -> list[tuple[str, int]] | None:
    """BFS shortest path on lead bipartite graph from start_clinic to end_clinic."""
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
            for lead_id in c2l.get(nid, set()):
                neighbor = ("lead", lead_id)
                if neighbor not in visited:
                    visited.add(neighbor)
                    parent[neighbor] = (ntype, nid)
                    queue.append(neighbor)
        else:
            for cli_id in l2c.get(nid, set()):
                neighbor = ("clinic", cli_id)
                if neighbor not in visited:
                    visited.add(neighbor)
                    parent[neighbor] = (ntype, nid)
                    if neighbor == end:
                        path = []
                        cur: tuple[str, int] | None = neighbor
                        while cur is not None:
                            path.append(cur)
                            cur = parent[cur]
                        return path[::-1]
                    queue.append(neighbor)

    return None


def _lead_path_similarity(path_a: list[tuple[str, int]], path_b: list[tuple[str, int]]) -> float:
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


def yen_k_lead_paths(
    c2l: dict[int, set[int]],
    l2c: dict[int, set[int]],
    start_clinic: int,
    end_clinic: int,
    k: int = 5,
    max_similarity: float = 0.7,
) -> list[list[tuple[str, int]]]:
    """Find k diverse shortest paths between two clinics via leads."""
    logger.info("Finding %d lead paths from clinic %d to clinic %d", k, start_clinic, end_clinic)

    shortest = _bfs_lead_shortest_path(c2l, l2c, start_clinic, end_clinic)
    if shortest is None:
        logger.info("No lead path found between clinic %d and %d", start_clinic, end_clinic)
        return []

    accepted: list[list[tuple[str, int]]] = [shortest]
    candidates: list[tuple[int, int, list[tuple[str, int]]]] = []
    candidate_counter = 0

    for ki in range(1, k * 3):
        prev_path = accepted[-1] if accepted else shortest

        for i in range(len(prev_path) - 1):
            spur_node = prev_path[i]
            root_path = prev_path[:i + 1]

            blocked: set[tuple[str, int]] = set()
            for path in accepted:
                if path[:i + 1] == root_path and i + 1 < len(path):
                    blocked.add(path[i + 1])

            for node in root_path[:-1]:
                blocked.add(node)

            spur_path = _bfs_lead_shortest_path(c2l, l2c, spur_node[1], end_clinic, blocked)
            if spur_path is not None:
                total_path = root_path[:-1] + spur_path
                heapq.heappush(candidates, (len(total_path), candidate_counter, total_path))
                candidate_counter += 1

        if not candidates:
            break

        while candidates:
            _, _, candidate = heapq.heappop(candidates)
            is_diverse = all(
                _lead_path_similarity(candidate, acc) < max_similarity
                for acc in accepted
            )
            if is_diverse:
                accepted.append(candidate)
                break

        if len(accepted) >= k:
            break

    logger.info("Found %d diverse lead paths", len(accepted))
    return accepted[:k]
