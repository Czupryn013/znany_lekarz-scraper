"""API endpoint handlers for lead-clinic network — thin orchestration layer."""

import logging

from fastapi import APIRouter, Depends, Query, Request
from sqlalchemy.orm import Session

from web_app.lead_graph import get_lead_neighborhood, yen_k_lead_paths
from web_app.lead_queries import (
    get_icp_clinic_details,
    get_lead_details,
    get_lead_metadata_batch,
    get_lead_clinic_roles_batch,
    get_random_connected_lead,
    search_lead_nodes,
)
from web_app.queries import get_clinic_specializations_batch

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/leads")


def _get_db(request: Request):
    """Yield a DB session per request."""
    session = request.app.state.SessionLocal()
    try:
        yield session
    finally:
        session.close()


def _lead_graph(request: Request, merge_nip: bool = False):
    """Return the in-memory lead graph adjacency dicts."""
    if merge_nip:
        return request.app.state.lead_merged_c2l, request.app.state.lead_merged_l2c
    return request.app.state.lead_c2l, request.app.state.lead_l2c


def _enrich_lead_nodes(
    nodes: list[dict], metadata: dict, specializations: dict[int, list[str]],
    roles: dict[str, list[str]], rep_to_members: dict[int, list[int]] | None = None,
) -> list[dict]:
    """Attach labels, contact info, and roles to graph nodes."""
    for node in nodes:
        rid = node["raw_id"]
        if node["type"] == "clinic":
            meta = metadata["clinics"].get(rid, {})
            node["label"] = meta.get("name", f"Clinic {rid}")
            node["doctors_count"] = meta.get("doctors_count")
            node["website"] = meta.get("website")
            node["linkedin"] = meta.get("linkedin")
            node["specializations"] = specializations.get(rid, [])
            if rep_to_members and rid in rep_to_members:
                node["locations_count"] = len(rep_to_members[rid])
        else:
            meta = metadata["leads"].get(rid, {})
            node["label"] = meta.get("full_name", f"Lead {rid}")
            node["phone"] = meta.get("phone")
            node["email"] = meta.get("email")
            node["linkedin_url"] = meta.get("linkedin_url")
            node["lead_source"] = meta.get("lead_source")
    return nodes


@router.get("/node/{node_type}/{node_id}")
def get_node(
    node_type: str,
    node_id: int,
    request: Request,
    merge_nip: bool = Query(default=False),
    db: Session = Depends(_get_db),
):
    """Get lead/clinic node details + 1-hop neighborhood."""
    c2l, l2c = _lead_graph(request, merge_nip)

    if node_type == "clinic":
        details = get_icp_clinic_details(db, node_id)
    elif node_type == "lead":
        details = get_lead_details(db, node_id)
    else:
        return {"error": "node_type must be 'clinic' or 'lead'"}

    if not details:
        return {"error": "not found"}

    neighborhood = get_lead_neighborhood(c2l, l2c, node_type, node_id, depth=1)

    clinic_ids = [n["raw_id"] for n in neighborhood["nodes"] if n["type"] == "clinic"]
    lead_ids = [n["raw_id"] for n in neighborhood["nodes"] if n["type"] == "lead"]
    metadata = get_lead_metadata_batch(db, clinic_ids, lead_ids)
    specs = get_clinic_specializations_batch(db, clinic_ids)
    roles = get_lead_clinic_roles_batch(db, lead_ids, clinic_ids)

    rep_members = request.app.state.lead_rep_to_members if merge_nip else None
    _enrich_lead_nodes(neighborhood["nodes"], metadata, specs, roles, rep_members)

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
    """Get subgraph around a lead/clinic node for visualization."""
    c2l, l2c = _lead_graph(request, merge_nip)
    neighborhood = get_lead_neighborhood(c2l, l2c, node_type, node_id, depth=depth)

    clinic_ids = [n["raw_id"] for n in neighborhood["nodes"] if n["type"] == "clinic"]
    lead_ids = [n["raw_id"] for n in neighborhood["nodes"] if n["type"] == "lead"]
    metadata = get_lead_metadata_batch(db, clinic_ids, lead_ids)
    specs = get_clinic_specializations_batch(db, clinic_ids)
    roles = get_lead_clinic_roles_batch(db, lead_ids, clinic_ids)

    rep_members = request.app.state.lead_rep_to_members if merge_nip else None
    _enrich_lead_nodes(neighborhood["nodes"], metadata, specs, roles, rep_members)

    return neighborhood


@router.get("/pathfind")
def pathfind(
    request: Request,
    from_clinic: int = Query(...),
    to_clinic: int = Query(...),
    k: int = Query(default=5, ge=1, le=20),
    merge_nip: bool = Query(default=False),
    db: Session = Depends(_get_db),
):
    """Find k diverse shortest paths between two ICP clinics via leads."""
    c2l, l2c = _lead_graph(request, merge_nip)

    if from_clinic not in c2l:
        return {"error": f"Clinic {from_clinic} not found in lead graph"}
    if to_clinic not in c2l:
        return {"error": f"Clinic {to_clinic} not found in lead graph"}

    raw_paths = yen_k_lead_paths(c2l, l2c, from_clinic, to_clinic, k=k)

    all_clinic_ids: set[int] = set()
    all_lead_ids: set[int] = set()
    for path in raw_paths:
        for ntype, nid in path:
            if ntype == "clinic":
                all_clinic_ids.add(nid)
            else:
                all_lead_ids.add(nid)

    metadata = get_lead_metadata_batch(db, list(all_clinic_ids), list(all_lead_ids))
    specs = get_clinic_specializations_batch(db, list(all_clinic_ids))

    paths = []
    for path in raw_paths:
        labeled_path = []
        for ntype, nid in path:
            node = {"type": ntype, "id": f"{'c' if ntype == 'clinic' else 'l'}_{nid}", "raw_id": nid}
            if ntype == "clinic":
                meta = metadata["clinics"].get(nid, {})
                node["label"] = meta.get("name", f"Clinic {nid}")
                node["specializations"] = specs.get(nid, [])
            else:
                meta = metadata["leads"].get(nid, {})
                node["label"] = meta.get("full_name", f"Lead {nid}")
                node["phone"] = meta.get("phone")
                node["email"] = meta.get("email")
                node["linkedin_url"] = meta.get("linkedin_url")
            labeled_path.append(node)
        paths.append(labeled_path)

    return {"paths": paths, "count": len(paths)}


@router.get("/search")
def search(
    q: str = Query(..., min_length=2),
    db: Session = Depends(_get_db),
):
    """Search ICP clinics and leads by name."""
    return search_lead_nodes(db, q)


@router.get("/random")
def random_lead(
    request: Request,
    min_connections: int = Query(default=2, ge=2, le=20),
    merge_nip: bool = Query(default=False),
    db: Session = Depends(_get_db),
):
    """Get a random lead with at least min_connections clinics + their neighborhood."""
    result = get_random_connected_lead(db, min_connections)
    if not result:
        return {"error": f"No leads found with {min_connections}+ connections"}

    lead_id = result["lead_id"]
    c2l, l2c = _lead_graph(request, merge_nip)
    neighborhood = get_lead_neighborhood(c2l, l2c, "lead", lead_id, depth=1)

    clinic_ids = [n["raw_id"] for n in neighborhood["nodes"] if n["type"] == "clinic"]
    lead_ids = [n["raw_id"] for n in neighborhood["nodes"] if n["type"] == "lead"]
    metadata = get_lead_metadata_batch(db, clinic_ids, lead_ids)
    specs = get_clinic_specializations_batch(db, clinic_ids)
    roles = get_lead_clinic_roles_batch(db, lead_ids, clinic_ids)

    rep_members = request.app.state.lead_rep_to_members if merge_nip else None
    _enrich_lead_nodes(neighborhood["nodes"], metadata, specs, roles, rep_members)

    return {
        "lead_id": lead_id,
        "clinic_count": result["clinic_count"],
        "neighborhood": neighborhood,
    }


@router.get("/stats")
def stats(request: Request, merge_nip: bool = Query(default=False)):
    """Return lead graph size info."""
    c2l, l2c = _lead_graph(request, merge_nip)
    edge_count = sum(len(leads) for leads in c2l.values())
    return {
        "clinics": len(c2l),
        "leads": len(l2c),
        "edges": edge_count,
    }
