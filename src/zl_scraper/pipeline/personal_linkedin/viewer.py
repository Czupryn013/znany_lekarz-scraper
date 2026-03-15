"""Export a self-contained HTML viewer for LinkedIn profile review + import decisions."""

import json
import re
from datetime import date, datetime
from pathlib import Path
from typing import Optional

from sqlalchemy.orm import Session

from zl_scraper.db.engine import SessionLocal
from zl_scraper.db.models import Clinic, ClinicLocation, Lead, LinkedInProfile, lead_clinic_roles
from zl_scraper.utils.logging import get_logger

logger = get_logger("personal_linkedin.viewer")


def _extract_city(address: Optional[str]) -> Optional[str]:
  """Extract a city-like token from an address string."""
  if not address:
    return None
  parts = [p.strip() for p in address.split(",") if p.strip()]
  candidate = parts[-1] if parts else address.strip()
  candidate = re.sub(r"\b\d{2}-\d{3}\b", "", candidate).strip(" -")
  return candidate or None


def _age_from_pesel(pesel: Optional[str]) -> Optional[int]:
    """Return age in full years parsed from PESEL, or None."""
    if not pesel:
        return None
    digits = "".join(ch for ch in pesel if ch.isdigit())
    if len(digits) != 11:
        return None
    yy = int(digits[0:2])
    mm_raw = int(digits[2:4])
    dd = int(digits[4:6])
    if 1 <= mm_raw <= 12:
        century, mm = 1900, mm_raw
    elif 21 <= mm_raw <= 32:
        century, mm = 2000, mm_raw - 20
    elif 41 <= mm_raw <= 52:
        century, mm = 2100, mm_raw - 40
    elif 61 <= mm_raw <= 72:
        century, mm = 2200, mm_raw - 60
    elif 81 <= mm_raw <= 92:
        century, mm = 1800, mm_raw - 80
    else:
        return None
    year = century + yy
    try:
        birth = date(year, mm, dd)
    except ValueError:
        return None
    today = date.today()
    age = today.year - birth.year
    if (today.month, today.day) < (birth.month, birth.day):
        age -= 1
    return age


def _get_lead_companies(session: Session, lead_id: int) -> list[dict]:
    """Get company rows for a lead with domain + clinic location context."""
    rows = (
        session.query(
            Clinic.id,
            Clinic.name,
            Clinic.legal_name,
            Clinic.website_domain,
            lead_clinic_roles.c.role,
            ClinicLocation.address,
        )
        .join(lead_clinic_roles, Clinic.id == lead_clinic_roles.c.clinic_id)
        .outerjoin(ClinicLocation, ClinicLocation.clinic_id == Clinic.id)
        .filter(lead_clinic_roles.c.lead_id == lead_id)
        .all()
    )

    companies: dict[tuple[int, str], dict] = {}
    for clinic_id, name, legal_name, domain, role, address in rows:
        key = (clinic_id, role or "")
        if key not in companies:
            companies[key] = {
                "name": legal_name or name or "?",
                "domain": domain,
                "role": role,
                "city": _extract_city(address),
                "address": address,
            }
            continue

        # Prefer keeping an entry with location context if first one had none.
        if not companies[key].get("address") and address:
            companies[key]["address"] = address
            companies[key]["city"] = _extract_city(address)

    return list(companies.values())


def _build_lead_groups(session: Session) -> list[dict]:
    """Query PENDING profiles grouped by lead, with context."""
    profiles = (
        session.query(LinkedInProfile)
    .filter(LinkedInProfile.review_status == "PENDING")
        .order_by(LinkedInProfile.lead_id, LinkedInProfile.id)
        .all()
    )

    if not profiles:
        return []

    # Group by lead_id
    groups: dict[int, list[LinkedInProfile]] = {}
    for p in profiles:
        groups.setdefault(p.lead_id, []).append(p)

    result = []
    for lead_id, lead_profiles in groups.items():
        lead = session.get(Lead, lead_id) if lead_id else None
        lead_name = lead.full_name if lead else "Unknown"
        age = _age_from_pesel(lead.pesel) if lead else None
        companies = _get_lead_companies(session, lead_id) if lead_id else []
        existing_url = lead.linkedin_url if lead else None

        # If the lead already has an accepted profile, skip it from manual review.
        if existing_url:
            continue

        profiles_data = []
        no_profiles_data = []
        for p in lead_profiles:
            raw = p.raw_profile or {}
            profile_payload = {
                "id": p.id,
                "url": p.linkedin_url,
                "firstName": p.first_name,
                "lastName": p.last_name,
                "headline": p.headline,
                "locationText": p.location_text,
                "countryCode": p.country_code,
                "pictureUrl": p.profile_picture_url,
                "currentCompany": p.current_company,
                "currentPosition": p.current_position,
                "connectionsCount": p.connections_count,
                "llmVerdict": p.llm_verdict,
                "searchContext": p.search_context,
                "experience": raw.get("experience", []),
                "education": raw.get("education", []),
                "skills": [s.get("name", s) if isinstance(s, dict) else s for s in (raw.get("skills") or raw.get("topSkills") or [])],
                "about": raw.get("about"),
                "openToWork": raw.get("openToWork", False),
            }

            if (p.llm_verdict or "").upper() == "NO":
                no_profiles_data.append(profile_payload)
            else:
                profiles_data.append(profile_payload)

        result.append({
            "leadId": lead_id,
            "leadName": lead_name,
            "age": age,
            "companies": companies,
            "existingUrl": existing_url,
            "profiles": profiles_data,
            "noProfiles": no_profiles_data,
        })

    # Merge duplicate leads with the same full_name (same person, multiple Lead rows).
    merged: dict[str, dict] = {}
    for entry in result:
        key = entry["leadName"].strip().lower()
        if key not in merged:
            merged[key] = entry
        else:
            existing = merged[key]
            seen_ids = {p["id"] for p in existing["profiles"]}
            for p in entry["profiles"]:
                if p["id"] not in seen_ids:
                    existing["profiles"].append(p)
                    seen_ids.add(p["id"])
            seen_no = {p["id"] for p in existing["noProfiles"]}
            for p in entry["noProfiles"]:
                if p["id"] not in seen_no:
                    existing["noProfiles"].append(p)
                    seen_no.add(p["id"])
            seen_cos = {(c["name"], c.get("domain")) for c in existing["companies"]}
            for c in entry["companies"]:
                if (c["name"], c.get("domain")) not in seen_cos:
                    existing["companies"].append(c)
                    seen_cos.add((c["name"], c.get("domain")))
            if existing["age"] is None and entry["age"] is not None:
                existing["age"] = entry["age"]

    return list(merged.values())


def export_viewer_html(output_path: str = "linkedin_viewer.html") -> str:
    """Generate a self-contained HTML file for reviewing LinkedIn profiles."""
    session = SessionLocal()
    try:
        groups = _build_lead_groups(session)
        total_profiles = sum(len(g["profiles"]) for g in groups)
        logger.info("Exporting viewer: %d leads, %d profiles", len(groups), total_profiles)

        html = _build_html(groups)
        Path(output_path).write_text(html, encoding="utf-8")
        logger.info("Viewer exported to %s", output_path)
        return output_path
    finally:
        session.close()


def import_review_decisions(json_path: str) -> dict:
  """Read decisions JSON and update DB. Returns a detailed import summary."""
  data = json.loads(Path(json_path).read_text(encoding="utf-8"))
  decisions = data if isinstance(data, list) else data.get("decisions", [])

  session = SessionLocal()
  try:
    processed = 0
    approved = 0
    rejected = 0
    skipped = 0
    missing_profiles = 0
    auto_rejected = 0
    linkedin_no_appended = 0
    leads_linkedin_set: set[int] = set()

    for decision in decisions:
      profile_id = decision.get("profileId")
      status = decision.get("status", "").upper()

      if not profile_id or status not in ("APPROVED", "REJECTED"):
        skipped += 1
        continue

      processed += 1
      profile = session.get(LinkedInProfile, profile_id)
      if not profile:
        logger.warning("Profile #%d not found, skipping", profile_id)
        skipped += 1
        missing_profiles += 1
        continue

      profile.review_status = status
      profile.reviewed_at = datetime.utcnow()

      lead = session.get(Lead, profile.lead_id) if profile.lead_id else None
      if lead is None:
        continue

      norm_url = profile.linkedin_url.strip().lower().rstrip("/")
      if status == "APPROVED":
        lead.linkedin_url = norm_url
        leads_linkedin_set.add(lead.id)

        # Reject other pending profiles for this lead.
        other = (
          session.query(LinkedInProfile)
          .filter(
            LinkedInProfile.lead_id == lead.id,
            LinkedInProfile.id != profile.id,
            LinkedInProfile.review_status == "PENDING",
          )
          .all()
        )
        for sibling in other:
          sibling.review_status = "REJECTED"
          sibling.reviewed_at = datetime.utcnow()
          auto_rejected += 1
        approved += 1
      else:
        # Append rejected profile URL to lead.linkedin_no for future exclusion.
        existing_no = [u.strip() for u in (lead.linkedin_no or "").split(",") if u.strip()]
        if norm_url not in existing_no:
          existing_no.append(norm_url)
          lead.linkedin_no = ",".join(existing_no)
          linkedin_no_appended += 1
        rejected += 1

    session.commit()
    logger.info(
      "Imported decisions: %d approved, %d rejected, %d skipped (processed=%d)",
      approved,
      rejected,
      skipped,
      processed,
    )
    return {
      "processed": processed,
      "approved": approved,
      "rejected": rejected,
      "skipped": skipped,
      "missing_profiles": missing_profiles,
      "leads_linkedin_set": len(leads_linkedin_set),
      "linkedin_no_appended": linkedin_no_appended,
      "auto_rejected": auto_rejected,
    }
  except Exception:
    session.rollback()
    raise
  finally:
    session.close()


def _build_html(groups: list[dict]) -> str:
    """Build the self-contained HTML viewer string."""
    data_json = json.dumps(groups, ensure_ascii=False, default=str)

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>LinkedIn Profile Reviewer</title>
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800;900&display=swap');
* {{ margin: 0; padding: 0; box-sizing: border-box; }}
body {{ font-family: 'Inter', -apple-system, sans-serif; background: #0a0a0a; color: #e0e0e0; min-height: 100vh; display: flex; flex-direction: column; align-items: center; overflow-x: hidden; }}

/* floating money particles */
.money-particle {{
  position: fixed; pointer-events: none; z-index: 9999;
  font-size: 18px; opacity: 0; animation: moneyFloat 2.5s ease-out forwards;
}}
@keyframes moneyFloat {{
  0% {{ opacity: 1; transform: translateY(0) rotate(0deg) scale(1); }}
  100% {{ opacity: 0; transform: translateY(-220px) rotate(40deg) scale(1.5); }}
}}

/* confetti burst */
.confetti {{
  position: fixed; pointer-events: none; z-index: 9999;
  width: 8px; height: 8px; border-radius: 2px;
  animation: confettiFall 1.2s ease-out forwards;
}}
@keyframes confettiFall {{
  0% {{ opacity: 1; transform: translateY(0) rotate(0deg); }}
  100% {{ opacity: 0; transform: translateY(200px) rotate(720deg); }}
}}

/* swipe overlay stamps */
.swipe-stamp {{
  position: fixed; top: 50%; left: 50%; transform: translate(-50%, -50%) scale(0) rotate(-15deg);
  font-size: 72px; font-weight: 900; text-transform: uppercase; letter-spacing: 6px;
  pointer-events: none; z-index: 100; opacity: 0;
}}
.swipe-stamp.approve {{ color: #4caf50; text-shadow: 0 0 40px rgba(76,175,80,0.5); animation: stampIn 0.5s ease-out forwards; }}
.swipe-stamp.reject {{ color: #f44336; text-shadow: 0 0 40px rgba(244,67,54,0.5); animation: stampIn 0.5s ease-out forwards; }}
@keyframes stampIn {{
  0% {{ opacity: 0; transform: translate(-50%, -50%) scale(3) rotate(-15deg); }}
  40% {{ opacity: 1; transform: translate(-50%, -50%) scale(1) rotate(-15deg); }}
  100% {{ opacity: 0; transform: translate(-50%, -50%) scale(1.1) rotate(-12deg); }}
}}

/* revenue popup */
.revenue-popup {{
  position: fixed; top: 20%; left: 50%; transform: translateX(-50%);
  font-size: 28px; font-weight: 900; color: #4caf50; pointer-events: none; z-index: 100;
  text-shadow: 0 0 20px rgba(76,175,80,0.6); opacity: 0;
  animation: revPop 1.5s ease-out forwards;
}}
@keyframes revPop {{
  0% {{ opacity: 0; transform: translateX(-50%) translateY(0) scale(0.5); }}
  20% {{ opacity: 1; transform: translateX(-50%) translateY(-10px) scale(1.1); }}
  100% {{ opacity: 0; transform: translateX(-50%) translateY(-80px) scale(1); }}
}}

/* header */
.header {{ width: 100%; padding: 12px 20px; background: linear-gradient(135deg, #111 0%, #1a1a2e 100%); border-bottom: 1px solid #222; display: flex; justify-content: space-between; align-items: center; position: sticky; top: 0; z-index: 10; }}
.header h1 {{ font-size: 18px; font-weight: 800; background: linear-gradient(135deg, #0a66c2, #00d4ff); -webkit-background-clip: text; -webkit-text-fill-color: transparent; }}
.progress {{ font-size: 13px; color: #888; }}
.progress .done {{ color: #4caf50; font-weight: 700; }}
.progress .rej {{ color: #f44336; font-weight: 700; }}
.controls {{ display: flex; gap: 8px; align-items: center; }}
.controls button {{ padding: 5px 14px; border-radius: 6px; border: 1px solid #333; background: #1a1a1a; color: #ccc; cursor: pointer; font-size: 12px; font-weight: 600; transition: all 0.2s; }}
.controls button:hover {{ background: #252525; transform: translateY(-1px); }}

/* container */
.container {{ width: 100%; max-width: 480px; padding: 16px; flex: 1; perspective: 1000px; }}

/* lead card */
.lead-card {{
  background: linear-gradient(145deg, #141414 0%, #1a1a2e 100%);
  border: 1px solid #2a2a3e; border-radius: 20px; margin-bottom: 12px; overflow: hidden;
  box-shadow: 0 8px 32px rgba(0,0,0,0.4), 0 0 0 1px rgba(255,255,255,0.03);
  transition: transform 0.4s cubic-bezier(0.25, 0.46, 0.45, 0.94), opacity 0.4s;
}}
.lead-card.swipe-left {{ transform: translateX(-150%) rotate(-20deg); opacity: 0; }}
.lead-card.swipe-right {{ transform: translateX(150%) rotate(20deg); opacity: 0; }}

.lead-header {{ padding: 14px 18px; background: rgba(255,255,255,0.02); border-bottom: 1px solid #222; }}
.lead-header h2 {{ font-size: 18px; font-weight: 700; color: #fff; }}
.lead-header .meta {{ font-size: 12px; color: #666; margin-top: 3px; }}
.lead-header .meta .age {{ color: #ffb74d; font-weight: 700; }}
.lead-header .companies {{ font-size: 11px; color: #666; margin-top: 6px; }}
.lead-header .companies .company-row {{ display: block; margin-top: 4px; }}
.lead-header .companies .company-name {{ color: #adb3bd; font-weight: 600; }}
.lead-header .companies .company-domain {{ color: #66bbff; text-decoration: none; margin-left: 4px; }}
.lead-header .companies .company-domain:hover {{ text-decoration: underline; }}
.lead-header .companies .company-meta {{ color: #7e8793; margin-left: 4px; }}
.lead-header .companies .company-address {{ display: block; color: #555; font-size: 10px; margin-top: 1px; padding-left: 2px; }}

/* reject all bar inside card */
.reject-all-bar {{ padding: 6px 18px; display: flex; align-items: center; justify-content: space-between; background: rgba(244,67,54,0.05); border-bottom: 1px solid #1e1e1e; }}
.reject-all-bar button {{ padding: 4px 14px; border-radius: 6px; border: 1px solid #f44336; background: transparent; color: #f44336; cursor: pointer; font-size: 11px; font-weight: 700; text-transform: uppercase; letter-spacing: 0.5px; transition: all 0.2s; }}
.reject-all-bar button:hover {{ background: #f44336; color: #fff; }}
.reject-all-bar .queue-label {{ font-size: 10px; text-transform: uppercase; color: #7c8a9d; letter-spacing: 0.7px; font-weight: 600; }}

/* profiles */
.profile {{ border-top: 1px solid #1e1e1e; padding: 12px 18px; display: flex; gap: 12px; transition: all 0.3s ease; }}
.profile:first-child {{ border-top: none; }}
.profile.approved {{ background: rgba(76, 175, 80, 0.1); border-left: 3px solid #4caf50; }}
.profile.rejected {{ background: rgba(244, 67, 54, 0.06); opacity: 0.35; border-left: 3px solid #f44336; }}
.profile .pic {{ flex-shrink: 0; width: 52px; height: 52px; border-radius: 50%; background: #222; overflow: hidden; display: flex; align-items: center; justify-content: center; border: 2px solid #333; }}
.profile .pic img {{ width: 100%; height: 100%; object-fit: cover; }}
.profile .pic .placeholder {{ font-size: 20px; color: #444; }}
.profile .info {{ flex: 1; min-width: 0; }}
.profile .info .name-row {{ display: flex; align-items: center; gap: 6px; flex-wrap: wrap; }}
.profile .info .name {{ font-size: 14px; font-weight: 700; color: #fff; }}
.profile .info .badge {{ font-size: 10px; padding: 2px 6px; border-radius: 4px; font-weight: 700; }}
.badge.yes {{ background: #1b5e20; color: #81c784; }}
.badge.maybe {{ background: #e65100; color: #ffb74d; }}
.badge.no {{ background: #b71c1c; color: #ef9a9a; }}
.badge.otw {{ background: #0d47a1; color: #64b5f6; }}
.profile .info .headline {{ font-size: 12px; color: #aaa; margin-top: 2px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }}
.profile .info .loc {{ font-size: 11px; color: #666; margin-top: 1px; }}
.profile .info a {{ color: #4fc3f7; text-decoration: none; font-size: 11px; }}
.profile .info a:hover {{ text-decoration: underline; }}

.exp-section {{ margin-top: 4px; }}
.exp-section .section-title {{ font-size: 10px; text-transform: uppercase; color: #555; letter-spacing: 0.5px; margin-top: 4px; margin-bottom: 2px; font-weight: 600; }}
.exp-item {{ font-size: 11px; color: #999; line-height: 1.4; }}
.exp-item .role {{ color: #ccc; }}
.exp-item .co {{ color: #888; }}
.exp-item .dur {{ color: #555; font-size: 10px; }}

.about-text {{ font-size: 11px; color: #777; margin-top: 3px; max-height: 40px; overflow: hidden; cursor: pointer; transition: max-height 0.3s; }}
.about-text.expanded {{ max-height: none; }}

.skills-row {{ margin-top: 3px; display: flex; flex-wrap: wrap; gap: 3px; }}
.skill-tag {{ font-size: 10px; background: #1a1a2e; color: #7986cb; padding: 1px 6px; border-radius: 3px; }}

/* tinder action buttons */
.profile .actions {{ display: flex; flex-direction: column; gap: 6px; flex-shrink: 0; justify-content: center; }}
.btn {{ width: 40px; height: 40px; border-radius: 50%; border: 2px solid; cursor: pointer; font-size: 18px; display: flex; align-items: center; justify-content: center; background: transparent; transition: all 0.2s cubic-bezier(0.25, 0.46, 0.45, 0.94); }}
.btn:active {{ transform: scale(0.9); }}
.btn-yes {{ border-color: #4caf50; color: #4caf50; }}
.btn-yes:hover {{ background: #4caf50; color: #fff; box-shadow: 0 0 20px rgba(76,175,80,0.4); transform: scale(1.1); }}
.btn-no {{ border-color: #f44336; color: #f44336; }}
.btn-no:hover {{ background: #f44336; color: #fff; box-shadow: 0 0 20px rgba(244,67,54,0.4); transform: scale(1.1); }}

/* footer */
.footer {{ width: 100%; padding: 10px 20px; background: linear-gradient(135deg, #111 0%, #1a1a2e 100%); border-top: 1px solid #222; display: flex; justify-content: space-between; align-items: center; position: sticky; bottom: 0; }}
.footer button {{ padding: 6px 16px; border-radius: 8px; border: none; cursor: pointer; font-size: 13px; font-weight: 700; transition: all 0.2s; }}
.footer button:active {{ transform: scale(0.95); }}
.btn-export {{ background: linear-gradient(135deg, #0a66c2, #00d4ff); color: #fff; }}
.btn-export:hover {{ box-shadow: 0 0 20px rgba(10,102,194,0.4); transform: translateY(-1px); }}
.btn-undo {{ background: #222; color: #aaa; border: 1px solid #333; }}
.btn-undo:hover {{ background: #333; }}
.btn-clear {{ background: #222; color: #ff9800; border: 1px solid #ff9800; }}
.btn-clear:hover {{ background: #ff9800; color: #000; }}
.nav-btn {{ background: #222; color: #ccc; border: 1px solid #333; }}
.nav-btn:hover {{ background: #333; }}

.empty {{ text-align: center; padding: 80px 20px; color: #555; }}
.empty h2 {{ color: #aaa; margin-bottom: 10px; font-size: 24px; }}
.empty .money-rain {{ font-size: 48px; margin-bottom: 16px; animation: pulse 2s ease-in-out infinite; }}
@keyframes pulse {{ 0%, 100% {{ transform: scale(1); }} 50% {{ transform: scale(1.1); }} }}

.section-divider {{ margin: 8px 18px 2px; font-size: 10px; text-transform: uppercase; color: #7c8a9d; letter-spacing: 0.7px; font-weight: 600; }}
.section-divider.no {{ color: #c78f8f; }}

/* ambient glow */
.card-glow {{
  position: absolute; top: 50%; left: 50%; width: 300px; height: 300px;
  background: radial-gradient(circle, rgba(10,102,194,0.08) 0%, transparent 70%);
  transform: translate(-50%, -50%); pointer-events: none; z-index: -1;
  animation: glowPulse 4s ease-in-out infinite;
}}
@keyframes glowPulse {{ 0%, 100% {{ opacity: 0.5; }} 50% {{ opacity: 1; }} }}
</style>
</head>
<body>

<div class="header">
  <h1>🔥 LinkedIn Matcher</h1>
  <div class="progress" id="progress"></div>
  <div class="controls">
    <button class="btn-undo" onclick="undo()" title="Undo last (Ctrl+Z)">↩ Undo</button>
    <button class="btn-export" onclick="exportDecisions()">💾 Export</button>
  </div>
</div>

<div class="container" id="container" style="position:relative;"></div>

<div class="footer">
  <div style="display:flex;gap:6px;">
    <button class="nav-btn" onclick="prevLead()">← Prev</button>
    <button class="nav-btn" onclick="nextLead()">Next →</button>
  </div>
  <div style="display:flex;gap:6px;">
    <button class="btn-clear" onclick="clearStorage()" title="Clear saved state">🗑 Reset</button>
    <button class="btn-export" onclick="exportDecisions()">💾 Export JSON</button>
  </div>
</div>

<script>
const LEADS = {data_json};
const KEY = 'li_viewer_decisions';
const KEY_IDX = 'li_viewer_idx';

let currentIdx = parseInt(localStorage.getItem(KEY_IDX) || '0', 10);
let decisions = JSON.parse(localStorage.getItem(KEY) || '{{}}');
let history = [];
let totalRevenue = 0;

// Auto-clear stale localStorage if the profile IDs don't match the current dataset.
(function validateStorage() {{
  const validIds = new Set();
  for (const g of LEADS) {{
    for (const p of [...g.profiles, ...(g.noProfiles || [])]) validIds.add(String(p.id));
  }}
  const storedIds = Object.keys(decisions);
  if (storedIds.length > 0 && storedIds.every(id => !validIds.has(id))) {{
    // All stored decisions reference IDs not in current data — stale session.
    decisions = {{}};
    currentIdx = 0;
    localStorage.removeItem(KEY);
    localStorage.removeItem(KEY_IDX);
  }}
  if (currentIdx >= LEADS.length) currentIdx = 0;
}})();

function getStats() {{
  let ap = 0, rj = 0, pend = 0;
  for (const g of LEADS) {{
    for (const p of [...g.profiles, ...(g.noProfiles || [])]) {{
      const d = decisions[p.id];
      if (d === 'APPROVED') ap++;
      else if (d === 'REJECTED') rj++;
      else pend++;
    }}
  }}
  return {{ ap, rj, pend, total: ap + rj + pend }};
}}

function updateProgress() {{
  const s = getStats();
  document.getElementById('progress').innerHTML =
    `<span class="done">${{s.ap}} ✓</span> &nbsp; <span class="rej">${{s.rj}} ✗</span> &nbsp; ${{s.pend}} pending &nbsp; (${{s.total}} total)`;
}}

function save() {{
  localStorage.setItem(KEY, JSON.stringify(decisions));
  localStorage.setItem(KEY_IDX, currentIdx.toString());
  updateProgress();
}}

function clearStorage() {{
  if (!confirm('Clear all decisions and reset position? This cannot be undone.')) return;
  localStorage.removeItem(KEY);
  localStorage.removeItem(KEY_IDX);
  decisions = {{}};
  currentIdx = 0;
  history = [];
  totalRevenue = 0;
  renderLead(0);
  updateProgress();
}}

/* $50k/mo animations */
function spawnMoney(x, y) {{
  const emojis = ['💰', '💵', '🤑', '💎', '📈', '🚀'];
  for (let i = 0; i < 6; i++) {{
    const el = document.createElement('div');
    el.className = 'money-particle';
    el.textContent = emojis[Math.floor(Math.random() * emojis.length)];
    el.style.left = (x + (Math.random() - 0.5) * 100) + 'px';
    el.style.top = (y + (Math.random() - 0.5) * 40) + 'px';
    el.style.animationDelay = (Math.random() * 0.3) + 's';
    document.body.appendChild(el);
    setTimeout(() => el.remove(), 3000);
  }}
}}

function spawnConfetti(x, y) {{
  const colors = ['#4caf50', '#00d4ff', '#ffb74d', '#e040fb', '#ff5252', '#448aff'];
  for (let i = 0; i < 20; i++) {{
    const el = document.createElement('div');
    el.className = 'confetti';
    el.style.background = colors[Math.floor(Math.random() * colors.length)];
    el.style.left = (x + (Math.random() - 0.5) * 200) + 'px';
    el.style.top = (y - 50 + Math.random() * 40) + 'px';
    el.style.animationDelay = (Math.random() * 0.2) + 's';
    document.body.appendChild(el);
    setTimeout(() => el.remove(), 1500);
  }}
}}

function showStamp(type) {{
  const el = document.createElement('div');
  el.className = 'swipe-stamp ' + type;
  el.textContent = type === 'approve' ? '✓ MATCH' : '✗ NOPE';
  document.body.appendChild(el);
  setTimeout(() => el.remove(), 600);
}}

function showRevenue() {{
  const amounts = ['$4,166', '$2,500', '$1,250', '$8,333', '$3,750'];
  const el = document.createElement('div');
  el.className = 'revenue-popup';
  el.textContent = '+' + amounts[Math.floor(Math.random() * amounts.length)] + '/mo 🔥';
  document.body.appendChild(el);
  setTimeout(() => el.remove(), 1600);
}}

function renderExperience(exp) {{
  if (!exp || !exp.length) return '';
  const items = exp.slice(0, 4).map(e => {{
    const pos = e.position || e.title || '';
    const co = e.companyName || '';
    const dur = e.duration || '';
    const loc = e.location || '';
    let s = `<div class="exp-item"><span class="role">${{esc(pos)}}</span>`;
    if (co) s += ` <span class="co">@ ${{esc(co)}}</span>`;
    if (dur) s += ` <span class="dur">&middot; ${{esc(dur)}}</span>`;
    if (loc) s += ` <span class="dur">&middot; ${{esc(loc)}}</span>`;
    s += '</div>';
    return s;
  }}).join('');
  return `<div class="section-title">Experience</div>${{items}}`;
}}

function renderEducation(edu) {{
  if (!edu || !edu.length) return '';
  const items = edu.slice(0, 3).map(e => {{
    const school = e.schoolName || e.school || '';
    const degree = e.degreeName || e.degree || '';
    const field = e.fieldOfStudy || e.field || '';
    let s = `<div class="exp-item"><span class="role">${{esc(school)}}</span>`;
    if (degree || field) s += ` <span class="co">${{esc([degree, field].filter(Boolean).join(', '))}}</span>`;
    s += '</div>';
    return s;
  }}).join('');
  return `<div class="section-title">Education</div>${{items}}`;
}}

function esc(s) {{ return s ? String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;') : ''; }}

function normalizeDomainUrl(domain) {{
  if (!domain) return '';
  const d = String(domain).trim();
  if (!d) return '';
  if (d.startsWith('http://') || d.startsWith('https://')) return d;
  return 'https://' + d;
}}

function renderProfile(p) {{
  const status = decisions[p.id] || '';
  const cls = status === 'APPROVED' ? 'approved' : status === 'REJECTED' ? 'rejected' : '';
  const verdictCls = (p.llmVerdict || '').toLowerCase();

  const pic = p.pictureUrl
    ? `<img src="${{esc(p.pictureUrl)}}" onerror="this.parentNode.innerHTML='<div class=placeholder>👤</div>'">`
    : '<div class="placeholder">👤</div>';

  const badges = [];
  if (p.llmVerdict) badges.push(`<span class="badge ${{verdictCls}}">${{p.llmVerdict}}</span>`);
  if (p.openToWork) badges.push('<span class="badge otw">OTW</span>');
  if (p.searchContext) badges.push(`<span class="badge" style="background:#222;color:#666">${{p.searchContext}}</span>`);

  const skills = (p.skills || []).slice(0, 8).map(s => `<span class="skill-tag">${{esc(typeof s === 'string' ? s : s.name || '')}}</span>`).join('');

  return `
  <div class="profile ${{cls}}" id="p-${{p.id}}">
    <div class="pic">${{pic}}</div>
    <div class="info">
      <div class="name-row">
        <span class="name">${{esc(p.firstName)}} ${{esc(p.lastName)}}</span>
        ${{badges.join('')}}
        ${{p.connectionsCount != null ? `<span style="font-size:10px;color:#555">${{p.connectionsCount}} conn</span>` : ''}}
      </div>
      <div class="headline">${{esc(p.headline)}}</div>
      ${{p.locationText ? `<div class="loc">📍 ${{esc(p.locationText)}}</div>` : ''}}
      <a href="${{esc(p.url)}}" target="_blank" rel="noopener">${{esc(p.url)}}</a>
      ${{p.about ? `<div class="about-text" onclick="this.classList.toggle('expanded')">${{esc(p.about)}}</div>` : ''}}
      <div class="exp-section">
        ${{renderExperience(p.experience)}}
        ${{renderEducation(p.education)}}
      </div>
      ${{skills ? `<div class="skills-row">${{skills}}</div>` : ''}}
    </div>
    <div class="actions">
      <button class="btn btn-yes" onclick="decide(${{p.id}},'APPROVED',event)" title="Approve">✓</button>
      <button class="btn btn-no" onclick="decide(${{p.id}},'REJECTED',event)" title="Reject">✗</button>
    </div>
  </div>`;
}}

function renderLead(idx) {{
  const c = document.getElementById('container');
  if (idx < 0 || idx >= LEADS.length) {{
    c.innerHTML = '<div class="empty"><div class="money-rain">🤑💰🚀</div><h2>All done! $50k/mo secured.</h2><p>Click Export to download decisions.</p></div>';
    return;
  }}
  const g = LEADS[idx];
  const ageStr = g.age != null ? `<span class="age">${{g.age}} yrs</span>` : '';
  const companiesHtml = g.companies.map(co => {{
    const domainUrl = normalizeDomainUrl(co.domain || '');
    const domainHtml = co.domain
      ? `<a class="company-domain" href="${{esc(domainUrl)}}" target="_blank" rel="noopener">${{esc(co.domain)}}</a>`
      : '';
    const roleText = co.role ? ` — ${{esc(co.role)}}` : '';
    const cityText = co.city ? ` • ${{esc(co.city)}}` : '';
    const addressLine = co.address ? `<span class="company-address">📍 ${{esc(co.address)}}</span>` : '';
    return `<div class="company-row"><span class="company-name">${{esc(co.name)}}</span>${{domainHtml}}<span class="company-meta">${{roleText}}${{cityText}}</span>${{addressLine}}</div>`;
  }}).join('');

  const profilesHtml = g.profiles.map(renderProfile).join('');
  const noProfiles = g.noProfiles || [];
  const noProfilesHtml = noProfiles.map(renderProfile).join('');
  const totalProfiles = g.profiles.length + noProfiles.length;

  c.innerHTML = `
  <div class="card-glow"></div>
  <div class="lead-card" id="lead-card">
    <div class="lead-header">
      <h2>${{esc(g.leadName)}} ${{ageStr}}</h2>
      <div class="meta">Lead #${{g.leadId}} &middot; ${{totalProfiles}} profile(s) &middot; Card ${{idx + 1}} / ${{LEADS.length}}</div>
      ${{companiesHtml ? `<div class="companies">${{companiesHtml}}</div>` : ''}}
    </div>
    <div class="reject-all-bar">
      <span class="queue-label">Review Queue</span>
      <button onclick="rejectAllForLead()">✗ Reject All</button>
    </div>
    ${{profilesHtml}}
    ${{noProfiles.length ? `<div class="section-divider no">LLM NO (double-check)</div>${{noProfilesHtml}}` : ''}}
  </div>`;
  c.scrollTop = 0;
  window.scrollTo(0, 0);
}}

function decide(profileId, status, evt) {{
  history.push({{ profileId, prev: decisions[profileId] || null }});
  decisions[profileId] = status;

  // Animations
  if (status === 'APPROVED') {{
    showStamp('approve');
    if (evt) {{ spawnMoney(evt.clientX, evt.clientY); spawnConfetti(evt.clientX, evt.clientY); }}
    else {{ spawnMoney(window.innerWidth / 2, window.innerHeight / 2); spawnConfetti(window.innerWidth / 2, window.innerHeight / 2); }}
    showRevenue();
  }} else {{
    showStamp('reject');
  }}

  // One approved profile finalizes the lead: reject siblings and advance.
  if (status === 'APPROVED' && currentIdx < LEADS.length) {{
    const g = LEADS[currentIdx];
    const siblings = [...g.profiles, ...(g.noProfiles || [])];
    for (const p of siblings) {{
      if (p.id !== profileId && !decisions[p.id]) {{
        decisions[p.id] = 'REJECTED';
      }}
    }}
  }}

  save();
  // Re-render in place
  const el = document.getElementById('p-' + profileId);
  if (el) {{
    const g = LEADS[currentIdx];
    const p = [...g.profiles, ...(g.noProfiles || [])].find(x => x.id === profileId);
    if (p) el.outerHTML = renderProfile(p);
  }}

  if (status === 'APPROVED') {{
    // Swipe card out then advance
    const card = document.getElementById('lead-card');
    if (card) {{
      card.classList.add('swipe-right');
      setTimeout(() => autoAdvance(), 400);
    }} else {{
      autoAdvance();
    }}
  }}
}}

function rejectAllForLead() {{
  if (currentIdx >= LEADS.length) return;
  showStamp('reject');
  const g = LEADS[currentIdx];
  for (const p of [...g.profiles, ...(g.noProfiles || [])]) {{
    if (!decisions[p.id] || decisions[p.id] === 'PENDING') {{
      history.push({{ profileId: p.id, prev: decisions[p.id] || null }});
      decisions[p.id] = 'REJECTED';
    }}
  }}
  save();
  const card = document.getElementById('lead-card');
  if (card) {{
    card.classList.add('swipe-left');
    setTimeout(() => autoAdvance(), 400);
  }} else {{
    autoAdvance();
  }}
}}

function nextLead() {{
  if (currentIdx < LEADS.length - 1) {{ currentIdx++; save(); renderLead(currentIdx); }}
}}

function prevLead() {{
  if (currentIdx > 0) {{ currentIdx--; save(); renderLead(currentIdx); }}
}}

function autoAdvance() {{
  for (let i = currentIdx + 1; i < LEADS.length; i++) {{
    const all = [...LEADS[i].profiles, ...(LEADS[i].noProfiles || [])];
    if (all.some(p => !decisions[p.id])) {{
      currentIdx = i; save(); renderLead(currentIdx); return;
    }}
  }}
  currentIdx = LEADS.length;
  save();
  renderLead(currentIdx);
}}

function undo() {{
  if (!history.length) return;
  const last = history.pop();
  if (last.prev) decisions[last.profileId] = last.prev;
  else delete decisions[last.profileId];
  save();
  renderLead(currentIdx);
}}

function exportDecisions() {{
  const out = [];
  for (const [pid, status] of Object.entries(decisions)) {{
    out.push({{ profileId: parseInt(pid, 10), status }});
  }}
  const blob = new Blob([JSON.stringify(out, null, 2)], {{ type: 'application/json' }});
  const a = document.createElement('a');
  a.href = URL.createObjectURL(blob);
  a.download = 'linkedin_decisions.json';
  a.click();
  URL.revokeObjectURL(a.href);
}}

// Keyboard shortcuts
document.addEventListener('keydown', (e) => {{
  if (e.key === 'ArrowRight') nextLead();
  else if (e.key === 'ArrowLeft') prevLead();
  else if (e.ctrlKey && e.key === 'z') {{ e.preventDefault(); undo(); }}
}});

renderLead(currentIdx);
updateProgress();
</script>
</body>
</html>"""
