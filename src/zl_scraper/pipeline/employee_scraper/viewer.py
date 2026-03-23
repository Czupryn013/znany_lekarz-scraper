"""Export a self-contained HTML viewer for employee review + import decisions."""

import json
from datetime import datetime
from pathlib import Path
from typing import Optional

from sqlalchemy.orm import Session

from zl_scraper.db.engine import SessionLocal
from zl_scraper.db.models import Clinic, ClinicLocation, Employee
from zl_scraper.utils.logging import get_logger

logger = get_logger("employee_scraper.viewer")


def _extract_city(address: Optional[str]) -> Optional[str]:
    """Extract a city-like token from an address string."""
    if not address:
        return None
    import re
    parts = [p.strip() for p in address.split(",") if p.strip()]
    candidate = parts[-1] if parts else address.strip()
    candidate = re.sub(r"\b\d{2}-\d{3}\b", "", candidate).strip(" -")
    return candidate or None


def _build_clinic_groups(session: Session) -> list[dict]:
    """Query PENDING employees grouped by clinic, with context."""
    employees = (
        session.query(Employee)
        .filter(Employee.review_status == "PENDING")
        .order_by(Employee.clinic_id, Employee.id)
        .all()
    )

    if not employees:
        return []

    # Group by clinic_id
    groups: dict[int, list[Employee]] = {}
    for e in employees:
        groups.setdefault(e.clinic_id, []).append(e)

    result = []
    for clinic_id, clinic_employees in groups.items():
        clinic = session.get(Clinic, clinic_id)
        if not clinic:
            continue

        # Get first location for context
        location = (
            session.query(ClinicLocation)
            .filter(ClinicLocation.clinic_id == clinic_id)
            .first()
        )

        employees_data = []
        for e in clinic_employees:
            employees_data.append({
                "id": e.id,
                "fullName": e.full_name,
                "linkedinUrl": e.linkedin_url,
                "positionTitle": e.position_title,
                "companyName": e.company_name,
            })

        result.append({
            "clinicId": clinic_id,
            "clinicName": clinic.legal_name or clinic.name or "?",
            "clinicLinkedin": clinic.linkedin_url,
            "domain": clinic.website_domain,
            "city": _extract_city(location.address if location else None),
            "employees": employees_data,
        })

    return result


def export_viewer_html(output_path: str = "employee_viewer.html") -> str:
    """Generate a self-contained HTML file for reviewing employees."""
    session = SessionLocal()
    try:
        groups = _build_clinic_groups(session)
        total_employees = sum(len(g["employees"]) for g in groups)
        logger.info("Exporting employee viewer: %d clinics, %d employees", len(groups), total_employees)

        html = _build_html(groups)
        Path(output_path).write_text(html, encoding="utf-8")
        logger.info("Employee viewer exported to %s", output_path)
        return output_path
    finally:
        session.close()


def import_review_decisions(json_path: str) -> dict:
    """Read decisions JSON and update DB. Returns import summary."""
    data = json.loads(Path(json_path).read_text(encoding="utf-8"))
    decisions = data if isinstance(data, list) else data.get("decisions", [])

    session = SessionLocal()
    try:
        approved = 0
        rejected = 0
        skipped = 0

        for decision in decisions:
            employee_id = decision.get("employeeId")
            status = decision.get("status", "").upper()

            if not employee_id or status not in ("APPROVED", "REJECTED"):
                skipped += 1
                continue

            employee = session.get(Employee, employee_id)
            if not employee:
                logger.warning("Employee #%d not found, skipping", employee_id)
                skipped += 1
                continue

            employee.review_status = status
            employee.reviewed_at = datetime.utcnow()

            if status == "APPROVED":
                approved += 1
            else:
                rejected += 1

        session.commit()
        logger.info(
            "Imported employee decisions: %d approved, %d rejected, %d skipped",
            approved, rejected, skipped,
        )
        return {"approved": approved, "rejected": rejected, "skipped": skipped}
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
<title>Employee Reviewer</title>
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800;900&display=swap');
* {{ margin: 0; padding: 0; box-sizing: border-box; }}
body {{ font-family: 'Inter', -apple-system, sans-serif; background: #0a0a0a; color: #e0e0e0; min-height: 100vh; display: flex; flex-direction: column; align-items: center; overflow-x: hidden; }}

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

.header {{ width: 100%; padding: 12px 20px; background: linear-gradient(135deg, #111 0%, #1a1a2e 100%); border-bottom: 1px solid #222; display: flex; justify-content: space-between; align-items: center; position: sticky; top: 0; z-index: 10; }}
.header h1 {{ font-size: 18px; font-weight: 800; background: linear-gradient(135deg, #0a66c2, #00d4ff); -webkit-background-clip: text; -webkit-text-fill-color: transparent; }}
.progress {{ font-size: 13px; color: #888; }}
.progress .done {{ color: #4caf50; font-weight: 700; }}
.progress .rej {{ color: #f44336; font-weight: 700; }}
.controls {{ display: flex; gap: 8px; align-items: center; }}
.controls button {{ padding: 5px 14px; border-radius: 6px; border: 1px solid #333; background: #1a1a1a; color: #ccc; cursor: pointer; font-size: 12px; font-weight: 600; transition: all 0.2s; }}
.controls button:hover {{ background: #252525; transform: translateY(-1px); }}

.main {{ width: 100%; max-width: 900px; padding: 20px; }}

.clinic-card {{ background: #141414; border: 1px solid #222; border-radius: 12px; margin-bottom: 20px; overflow: hidden; }}
.clinic-header {{ padding: 16px 20px; background: linear-gradient(135deg, #1a1a2e, #111); border-bottom: 1px solid #222; display: flex; justify-content: space-between; align-items: center; }}
.clinic-name {{ font-size: 16px; font-weight: 700; }}
.clinic-meta {{ font-size: 12px; color: #666; margin-top: 4px; }}
.clinic-meta a {{ color: #0a66c2; text-decoration: none; }}
.clinic-meta a:hover {{ text-decoration: underline; }}
.clinic-actions {{ display: flex; gap: 6px; }}
.clinic-actions button {{ padding: 4px 12px; border-radius: 4px; border: 1px solid #333; background: #1a1a1a; color: #ccc; cursor: pointer; font-size: 11px; font-weight: 600; }}
.btn-accept-all {{ border-color: #2e7d32 !important; color: #4caf50 !important; }}
.btn-reject-all {{ border-color: #c62828 !important; color: #f44336 !important; }}
.btn-accept-all:hover {{ background: #1b3d1b !important; }}
.btn-reject-all:hover {{ background: #3d1b1b !important; }}

.employee-row {{ display: flex; align-items: center; padding: 10px 20px; border-bottom: 1px solid #1a1a1a; transition: all 0.2s; }}
.employee-row:last-child {{ border-bottom: none; }}
.employee-row:hover {{ background: #1a1a1a; }}
.employee-row.decided {{ opacity: 0.5; }}
.employee-row.decided.approved {{ border-left: 3px solid #4caf50; }}
.employee-row.decided.rejected {{ border-left: 3px solid #f44336; }}

.emp-info {{ flex: 1; }}
.emp-name {{ font-size: 14px; font-weight: 600; }}
.emp-name a {{ color: #0a66c2; text-decoration: none; }}
.emp-name a:hover {{ text-decoration: underline; }}
.emp-title {{ font-size: 12px; color: #888; margin-top: 2px; }}

.emp-actions {{ display: flex; gap: 6px; }}
.emp-actions button {{ width: 32px; height: 32px; border-radius: 50%; border: 2px solid #333; background: #1a1a1a; cursor: pointer; font-size: 16px; display: flex; align-items: center; justify-content: center; transition: all 0.2s; }}
.btn-approve {{ color: #4caf50; }}
.btn-approve:hover {{ background: #1b3d1b; border-color: #4caf50; transform: scale(1.1); }}
.btn-reject {{ color: #f44336; }}
.btn-reject:hover {{ background: #3d1b1b; border-color: #f44336; transform: scale(1.1); }}

.empty {{ text-align: center; padding: 60px 20px; color: #555; font-size: 16px; }}
</style>
</head>
<body>

<div class="header">
  <div>
    <h1>Employee Reviewer</h1>
    <div class="progress" id="progress"></div>
  </div>
  <div class="controls">
    <button onclick="saveDecisions()">💾 Save</button>
    <button onclick="resetAll()">↺ Reset</button>
  </div>
</div>

<div class="main" id="main"></div>

<script>
const DATA = {data_json};
const decisions = {{}};
let totalEmployees = 0;

function init() {{
  const main = document.getElementById('main');
  if (!DATA.length) {{
    main.innerHTML = '<div class="empty">No pending employees to review.</div>';
    return;
  }}

  DATA.forEach(clinic => {{
    totalEmployees += clinic.employees.length;

    const card = document.createElement('div');
    card.className = 'clinic-card';
    card.id = 'clinic-' + clinic.clinicId;

    let metaParts = [];
    if (clinic.domain) metaParts.push(clinic.domain);
    if (clinic.city) metaParts.push(clinic.city);
    const metaStr = metaParts.join(' · ');

    let linkedinLink = '';
    if (clinic.clinicLinkedin) {{
      linkedinLink = `<a href="${{clinic.clinicLinkedin}}" target="_blank">LinkedIn</a>`;
    }}

    card.innerHTML = `
      <div class="clinic-header">
        <div>
          <div class="clinic-name">${{clinic.clinicName}}</div>
          <div class="clinic-meta">${{metaStr}}${{linkedinLink ? ' · ' + linkedinLink : ''}} · ${{clinic.employees.length}} employees</div>
        </div>
        <div class="clinic-actions">
          <button class="btn-accept-all" onclick="bulkDecide(${{clinic.clinicId}}, 'APPROVED')">✓ All</button>
          <button class="btn-reject-all" onclick="bulkDecide(${{clinic.clinicId}}, 'REJECTED')">✗ All</button>
        </div>
      </div>
    `;

    clinic.employees.forEach(emp => {{
      const row = document.createElement('div');
      row.className = 'employee-row';
      row.id = 'emp-' + emp.id;

      const titleStr = emp.positionTitle ? `${{emp.positionTitle}}${{emp.companyName ? ' at ' + emp.companyName : ''}}` : (emp.companyName || '');

      row.innerHTML = `
        <div class="emp-info">
          <div class="emp-name"><a href="${{emp.linkedinUrl}}" target="_blank">${{emp.fullName}}</a></div>
          <div class="emp-title">${{titleStr}}</div>
        </div>
        <div class="emp-actions">
          <button class="btn-approve" onclick="decide(${{emp.id}}, 'APPROVED')" title="Approve">✓</button>
          <button class="btn-reject" onclick="decide(${{emp.id}}, 'REJECTED')" title="Reject">✗</button>
        </div>
      `;
      card.appendChild(row);
    }});

    main.appendChild(card);
  }});

  updateProgress();
}}

function decide(empId, status) {{
  const prev = decisions[empId];
  if (prev === status) {{
    delete decisions[empId];
  }} else {{
    decisions[empId] = status;
  }}
  updateRow(empId);
  updateProgress();
}}

function bulkDecide(clinicId, status) {{
  const clinic = DATA.find(c => c.clinicId === clinicId);
  if (!clinic) return;

  // Show stamp animation
  const stamp = document.createElement('div');
  stamp.className = 'swipe-stamp ' + (status === 'APPROVED' ? 'approve' : 'reject');
  stamp.textContent = status === 'APPROVED' ? 'APPROVED' : 'REJECTED';
  document.body.appendChild(stamp);
  setTimeout(() => stamp.remove(), 600);

  clinic.employees.forEach(emp => {{
    decisions[emp.id] = status;
    updateRow(emp.id);
  }});
  updateProgress();
}}

function updateRow(empId) {{
  const row = document.getElementById('emp-' + empId);
  if (!row) return;
  const status = decisions[empId];
  row.className = 'employee-row' + (status ? ' decided ' + status.toLowerCase() : '');
}}

function updateProgress() {{
  const approved = Object.values(decisions).filter(s => s === 'APPROVED').length;
  const rejected = Object.values(decisions).filter(s => s === 'REJECTED').length;
  const remaining = totalEmployees - approved - rejected;
  document.getElementById('progress').innerHTML =
    `<span class="done">${{approved}} approved</span> · <span class="rej">${{rejected}} rejected</span> · ${{remaining}} remaining`;
}}

function resetAll() {{
  Object.keys(decisions).forEach(k => delete decisions[k]);
  document.querySelectorAll('.employee-row').forEach(r => r.className = 'employee-row');
  updateProgress();
}}

function saveDecisions() {{
  const list = Object.entries(decisions).map(([id, status]) => ({{ employeeId: parseInt(id), status }}));
  if (!list.length) {{ alert('No decisions to save.'); return; }}

  const blob = new Blob([JSON.stringify({{ decisions: list }}, null, 2)], {{ type: 'application/json' }});
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = 'employee_decisions.json';
  a.click();
  URL.revokeObjectURL(url);
}}

init();
</script>
</body>
</html>"""
