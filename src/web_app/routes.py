"""API endpoint handlers — thin orchestration connecting graph and DB queries."""

import logging

from fastapi import APIRouter, Depends, Query, Request
from sqlalchemy.orm import Session

from web_app.graph import get_neighborhood, yen_k_shortest_paths, find_doctors_by_specialization
from web_app.queries import (
    get_booking_ratio_batch,
    get_clinic_details,
    get_clinic_specializations_batch,
    get_doctor_details,
    get_doctor_specializations_batch,
    get_node_metadata_batch,
    search_nodes,
    search_specializations,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api")


def _get_db(request: Request):
    """Yield a DB session per request."""
    session = request.app.state.SessionLocal()
    try:
        yield session
    finally:
        session.close()


def _graph(request: Request, merge_nip: bool = False):
    """Return the in-memory graph adjacency dicts."""
    if merge_nip:
        return request.app.state.merged_c2d, request.app.state.merged_d2c
    return request.app.state.c2d, request.app.state.d2c


def _enrich_nodes(
    nodes: list[dict], metadata: dict,
    clinic_specs: dict[int, list[str]],
    doctor_specs: dict[int, list[str]] | None = None,
    rep_to_members: dict[int, list[int]] | None = None,
) -> list[dict]:
    """Attach labels, specializations, gender, and img_url to graph nodes."""
    for node in nodes:
        rid = node["raw_id"]
        if node["type"] == "clinic":
            meta = metadata["clinics"].get(rid, {})
            node["label"] = meta.get("name", f"Clinic {rid}")
            node["doctors_count"] = meta.get("doctors_count")
            node["specializations"] = clinic_specs.get(rid, [])
            if rep_to_members and rid in rep_to_members:
                node["locations_count"] = len(rep_to_members[rid])
        else:
            meta = metadata["doctors"].get(rid, {})
            name = meta.get("name", "")
            surname = meta.get("surname", "")
            node["label"] = f"{name} {surname}".strip() or f"Doctor {rid}"
            node["gender"] = meta.get("gender")
            node["img_url"] = meta.get("img_url")
            if doctor_specs:
                node["specializations"] = doctor_specs.get(rid, [])
    return nodes


@router.get("/node/{node_type}/{node_id}")
def get_node(
    node_type: str,
    node_id: int,
    request: Request,
    merge_nip: bool = Query(default=False),
    db: Session = Depends(_get_db),
):
    """Get node details + 1-hop neighborhood."""
    c2d, d2c = _graph(request, merge_nip)

    if node_type == "clinic":
        details = get_clinic_details(db, node_id)
    elif node_type == "doctor":
        details = get_doctor_details(db, node_id)
    else:
        return {"error": "node_type must be 'clinic' or 'doctor'"}

    if not details:
        return {"error": "not found"}

    neighborhood = get_neighborhood(c2d, d2c, node_type, node_id, depth=1)

    clinic_ids = [n["raw_id"] for n in neighborhood["nodes"] if n["type"] == "clinic"]
    doctor_ids = [n["raw_id"] for n in neighborhood["nodes"] if n["type"] == "doctor"]
    metadata = get_node_metadata_batch(db, clinic_ids, doctor_ids)
    clinic_specs = get_clinic_specializations_batch(db, clinic_ids)
    doc_specs = get_doctor_specializations_batch(db, doctor_ids)

    # Attach booking_ratio to edges
    pairs = [(int(e["source"].split("_")[1]), int(e["target"].split("_")[1]))
             for e in neighborhood["edges"] if e["source"].startswith("c_")]
    pairs += [(int(e["target"].split("_")[1]), int(e["source"].split("_")[1]))
              for e in neighborhood["edges"] if e["target"].startswith("c_")]
    booking_map = get_booking_ratio_batch(db, pairs)
    for edge in neighborhood["edges"]:
        pair = [edge["source"], edge["target"]]
        pair.sort()
        key = f"{pair[0]}-{pair[1]}"
        bdata = booking_map.get(key, {})
        if bdata.get("booking_ratio") is not None:
            edge["booking_ratio"] = bdata["booking_ratio"]

    rep_members = request.app.state.rep_to_members if merge_nip else None
    _enrich_nodes(neighborhood["nodes"], metadata, clinic_specs, doc_specs, rep_members)

    return {"details": details, "neighborhood": neighborhood}


@router.get("/neighbors/{node_type}/{node_id}")
def get_neighbors(
    node_type: str,
    node_id: int,
    request: Request,
    depth: int = Query(default=1, ge=1, le=3),
    merge_nip: bool = Query(default=False),
    db: Session = Depends(_get_db),
):
    """Get subgraph around a node for visualization."""
    c2d, d2c = _graph(request, merge_nip)
    neighborhood = get_neighborhood(c2d, d2c, node_type, node_id, depth=depth)

    clinic_ids = [n["raw_id"] for n in neighborhood["nodes"] if n["type"] == "clinic"]
    doctor_ids = [n["raw_id"] for n in neighborhood["nodes"] if n["type"] == "doctor"]
    metadata = get_node_metadata_batch(db, clinic_ids, doctor_ids)
    clinic_specs = get_clinic_specializations_batch(db, clinic_ids)
    doc_specs = get_doctor_specializations_batch(db, doctor_ids)

    rep_members = request.app.state.rep_to_members if merge_nip else None
    _enrich_nodes(neighborhood["nodes"], metadata, clinic_specs, doc_specs, rep_members)

    return neighborhood


@router.get("/pathfind")
def pathfind(
    request: Request,
    from_clinic: str | None = Query(default=None, description="Comma-separated clinic IDs"),
    from_doctor: int | None = Query(default=None),
    to_clinic: int | None = Query(default=None),
    to_doctor: int | None = Query(default=None),
    k: int = Query(default=5, ge=1, le=20),
    merge_nip: bool = Query(default=False),
    db: Session = Depends(_get_db),
):
    """Find k diverse shortest paths between doctor/clinic endpoints."""
    c2d, d2c = _graph(request, merge_nip)

    # Resolve "from" to clinic IDs
    from_doctor_id = None
    if from_doctor is not None:
        from_doctor_id = from_doctor
        from_ids = list(d2c.get(from_doctor, set()))
        if not from_ids:
            return {"error": f"Doctor {from_doctor} has no clinic connections"}
    elif from_clinic is not None:
        from_ids = []
        for s in from_clinic.split(","):
            s = s.strip()
            if s.isdigit():
                from_ids.append(int(s))
        if not from_ids:
            return {"error": "No valid from_clinic IDs provided"}
    else:
        return {"error": "Provide from_clinic or from_doctor"}

    # Resolve "to" to clinic IDs
    to_doctor_id = None
    if to_doctor is not None:
        to_doctor_id = to_doctor
        to_clinic_ids = list(d2c.get(to_doctor, set()))
        if not to_clinic_ids:
            return {"error": f"Doctor {to_doctor} has no clinic connections"}
    elif to_clinic is not None:
        to_clinic_ids = [to_clinic]
    else:
        return {"error": "Provide to_clinic or to_doctor"}

    # Validate clinic existence
    for cid in from_ids:
        if cid not in c2d:
            return {"error": f"Clinic {cid} not found in graph"}
    for cid in to_clinic_ids:
        if cid not in c2d:
            return {"error": f"Clinic {cid} not found in graph"}

    # Run pathfinding from each start clinic to each target clinic
    raw_paths = []
    for fid in from_ids:
        for tid in to_clinic_ids:
            raw_paths.extend(yen_k_shortest_paths(c2d, d2c, fid, tid, k=k))
    raw_paths.sort(key=len)
    raw_paths = raw_paths[:k]

    # Prepend/append doctor nodes to paths if doctor endpoints were used
    if from_doctor_id is not None:
        for i, path in enumerate(raw_paths):
            if path and path[0] != ("doctor", from_doctor_id):
                raw_paths[i] = [("doctor", from_doctor_id)] + list(path)
    if to_doctor_id is not None:
        for i, path in enumerate(raw_paths):
            if path and path[-1] != ("doctor", to_doctor_id):
                raw_paths[i] = list(path) + [("doctor", to_doctor_id)]

    # Collect all node IDs for batch metadata fetch
    all_clinic_ids: set[int] = set()
    all_doctor_ids: set[int] = set()
    for path in raw_paths:
        for ntype, nid in path:
            if ntype == "clinic":
                all_clinic_ids.add(nid)
            else:
                all_doctor_ids.add(nid)

    metadata = get_node_metadata_batch(db, list(all_clinic_ids), list(all_doctor_ids))
    specs = get_clinic_specializations_batch(db, list(all_clinic_ids))

    paths = []
    for path in raw_paths:
        labeled_path = []
        for ntype, nid in path:
            node = {"type": ntype, "id": f"{'c' if ntype == 'clinic' else 'd'}_{nid}", "raw_id": nid}
            if ntype == "clinic":
                meta = metadata["clinics"].get(nid, {})
                node["label"] = meta.get("name", f"Clinic {nid}")
                node["specializations"] = specs.get(nid, [])
            else:
                meta = metadata["doctors"].get(nid, {})
                name = meta.get("name", "")
                surname = meta.get("surname", "")
                node["label"] = f"{name} {surname}".strip() or f"Doctor {nid}"
            labeled_path.append(node)
        paths.append(labeled_path)

    return {"paths": paths, "count": len(paths)}


@router.get("/search")
def search(
    q: str = Query(..., min_length=2),
    db: Session = Depends(_get_db),
):
    """Search clinics and doctors by name."""
    return search_nodes(db, q)


@router.get("/search-specializations")
def search_specs(
    q: str = Query(..., min_length=2),
    db: Session = Depends(_get_db),
):
    """Autocomplete search for specializations."""
    return search_specializations(db, q)


@router.get("/find-by-specialization")
def find_by_spec(
    request: Request,
    from_clinic: int | None = Query(default=None),
    from_doctor: int | None = Query(default=None),
    spec_ids: str = Query(..., description="Comma-separated specialization IDs"),
    hops: int = Query(default=3, ge=1, le=8),
    merge_nip: bool = Query(default=False),
    db: Session = Depends(_get_db),
):
    """Find doctors with target specializations reachable from a clinic or doctor."""
    c2d, d2c = _graph(request, merge_nip)
    doctor_specs_map = request.app.state.doctor_specs

    # Resolve start point
    start_clinics = []
    start_doctors = None
    if from_doctor is not None:
        if from_doctor not in d2c:
            return {"error": f"Doctor {from_doctor} not found in graph"}
        start_doctors = [from_doctor]
    elif from_clinic is not None:
        if from_clinic not in c2d:
            return {"error": f"Clinic {from_clinic} not found in graph"}
        start_clinics = [from_clinic]
    else:
        return {"error": "Provide from_clinic or from_doctor"}

    target_ids = set()
    for s in spec_ids.split(","):
        s = s.strip()
        if s.isdigit():
            target_ids.add(int(s))

    if not target_ids:
        return {"error": "No valid specialization IDs provided"}

    logger.info(
        "Spec search: from=%s, specs=%s, hops=%d, doctor_specs_size=%d",
        f"doctor={from_doctor}" if from_doctor else f"clinic={from_clinic}",
        target_ids, hops, len(doctor_specs_map),
    )

    result = find_doctors_by_specialization(
        c2d, d2c, start_clinics, target_ids, doctor_specs_map,
        max_hops=hops, start_doctors=start_doctors,
    )
    logger.info("Spec search result: %d doctors found", len(result["results"]))

    # Enrich nodes
    clinic_ids = [n["raw_id"] for n in result["nodes"] if n["type"] == "clinic"]
    doctor_ids = [n["raw_id"] for n in result["nodes"] if n["type"] == "doctor"]
    metadata = get_node_metadata_batch(db, clinic_ids, doctor_ids)
    clinic_specs = get_clinic_specializations_batch(db, clinic_ids)
    doc_specs = get_doctor_specializations_batch(db, doctor_ids)

    rep_members = request.app.state.rep_to_members if merge_nip else None
    _enrich_nodes(result["nodes"], metadata, clinic_specs, doc_specs, rep_members)

    # Add spec names to results
    spec_names = _get_spec_names(db, target_ids)
    for r in result["results"]:
        r["matching_specs"] = [spec_names.get(sid, str(sid)) for sid in r["matching_spec_ids"]]

    return result


def _get_spec_names(db: Session, spec_ids: set[int]) -> dict[int, str]:
    """Fetch specialization names by IDs."""
    from sqlalchemy import text
    if not spec_ids:
        return {}
    rows = db.execute(
        text("SELECT id, name FROM specializations WHERE id = ANY(:ids)"),
        {"ids": list(spec_ids)},
    ).fetchall()
    return {r[0]: r[1] for r in rows}


@router.get("/stats")
def stats(request: Request, merge_nip: bool = Query(default=False)):
    """Return graph size info."""
    c2d, d2c = _graph(request, merge_nip)
    edge_count = sum(len(docs) for docs in c2d.values())
    doctor_specs_map = request.app.state.doctor_specs
    docs_with_specs = len(doctor_specs_map)
    return {
        "clinics": len(c2d),
        "doctors": len(d2c),
        "edges": edge_count,
        "doctors_with_specs": docs_with_specs,
    }
