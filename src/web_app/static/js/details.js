import { state } from './app.js';
import { apiFetch, mergeParamFirst, esc, genderLabel, genderIcon } from './utils.js';

export async function loadNodeDetails(type, rawId) {
    const data = await apiFetch(`/node/${type}/${rawId}${mergeParamFirst()}`);
    if (data.error) return;
    const det = data.details;
    const content = document.getElementById('right-panel-content');
    const cy = state.cy;

    let html = '';
    if (type === 'clinic') {
        html += `<h3 style="color:var(--accent)">${esc(det.name || 'Unknown')}</h3>`;
        if (det.locations && det.locations.length) {
            const addr = det.locations[0].address;
            if (addr) html += `<div style="font-size:11px;color:var(--text-dim);margin-top:2px">${esc(addr)}</div>`;
        }
        if (det.doctors_count) {
            let docLine = `<span class="detail-label">Doctors:</span> ${det.doctors_count}`;
            if (state.specMatchDoctorIds.size) {
                const clinicNode = cy.getElementById(`c_${rawId}`);
                if (clinicNode.length) {
                    let matchCount = 0;
                    clinicNode.connectedEdges().connectedNodes().forEach(n => {
                        if (n.data('type') === 'doctor' && state.specMatchDoctorIds.has(n.id())) matchCount++;
                    });
                    if (matchCount > 0) docLine += ` <span style="color:#eab308;font-weight:600">(${matchCount} matching)</span>`;
                }
            }
            html += `<div class="detail-row">${docLine}</div>`;
        }
        if (det.legal_name) html += `<div class="detail-row"><span class="detail-label">Legal:</span> ${esc(det.legal_name)}</div>`;
        if (det.nip) html += `<div class="detail-row"><span class="detail-label">NIP:</span> ${det.nip}</div>`;
        if (det.website_domain) html += `<div class="detail-row"><span class="detail-label">Website:</span> ${esc(det.website_domain)}</div>`;
        if (det.zl_url) html += `<div class="detail-row"><a href="${esc(det.zl_url)}" target="_blank" style="color:var(--accent);font-size:12px">ZnanyLekarz profile</a></div>`;
        if (det.locations && det.locations.length) {
            html += '<div class="detail-row" style="margin-top:6px"><span class="detail-label">Locations:</span></div>';
            for (const loc of det.locations) if (loc.address) html += `<div class="detail-row" style="padding-left:8px;font-size:12px;color:var(--text-muted)">${esc(loc.address)}</div>`;
        }
        if (det.specializations && det.specializations.length) {
            html += '<div style="margin-top:6px">';
            for (const s of det.specializations) html += `<span class="spec-tag">${esc(s)}</span>`;
            html += '</div>';
        }
    } else {
        html += '<div class="doc-header">';
        if (det.img_url) html += `<img src="${esc(det.img_url)}" onerror="this.style.display='none'">`;
        html += '<div class="doc-header-info">';
        html += `<h3 style="color:var(--green)">${esc(det.name || '')} ${esc(det.surname || '')}</h3>`;
        if (det.gender != null) html += `<div style="font-size:12px;color:var(--text-dim)">${genderIcon(det.gender)} ${genderLabel(det.gender)}</div>`;
        html += '</div></div>';
        if (det.zl_url) html += `<div class="detail-row"><a href="${esc(det.zl_url)}" target="_blank" style="color:var(--accent);font-size:12px">ZnanyLekarz profile</a></div>`;

        if (det.specializations && det.specializations.length) {
            html += '<div style="margin-top:8px">';
            for (const s of det.specializations) {
                const cls = s.is_in_progress ? 'spec-tag in-progress' : 'spec-tag';
                html += `<span class="${cls}">${esc(s.name)}${s.is_in_progress ? ' (w trakcie)' : ''}</span>`;
            }
            html += '</div>';
        }

        const pos = det.opinions_positive || 0, neu = det.opinions_neutral || 0, neg = det.opinions_negative || 0;
        const total = pos + neu + neg;
        if (total > 0) {
            const pP = (pos/total*100).toFixed(0), nP = (neu/total*100).toFixed(0), gP = (neg/total*100).toFixed(0);
            html += `<div style="margin-top:10px"><span class="detail-label">Reviews (${total})</span></div>`;
            html += `<div class="reviews-bar"><div class="bar"><div class="bar-pos" style="width:${pP}%"></div><div class="bar-neu" style="width:${nP}%"></div><div class="bar-neg" style="width:${gP}%"></div></div></div>`;
            html += `<div class="reviews-nums"><span style="color:var(--green)">+${pos}</span><span style="color:var(--yellow)">~${neu}</span><span style="color:var(--red)">-${neg}</span></div>`;
        }

        if (det.clinic_bookings && det.clinic_bookings.length) {
            html += '<div style="margin-top:10px"><span class="detail-label">Booking per clinic</span></div>';
            for (const cb of det.clinic_bookings) {
                const pct = cb.booking_ratio != null ? (cb.booking_ratio * 100).toFixed(0) : '?';
                const fw = cb.booking_ratio != null ? (cb.booking_ratio * 100) : 0;
                html += `<div class="booking-row"><div class="booking-bar"><div class="booking-bar-fill" style="width:${fw}%"></div></div>`;
                html += `<span style="color:var(--accent);min-width:28px">${pct}%</span>`;
                html += `<span style="flex:1;color:var(--text-muted);font-size:11px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${esc(cb.clinic_name)}</span>`;
                if (cb.is_bookable) html += '<span style="color:var(--green);font-size:10px">bookable</span>';
                html += '</div>';
            }
        }
    }
    content.innerHTML = html;
}
