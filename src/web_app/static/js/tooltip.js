import { state } from './app.js';
import { esc, genderLabel, genderIcon } from './utils.js';

export function setupTooltips() {
    const cy = state.cy;
    const tooltip = document.getElementById('tooltip');

    cy.on('mouseover', 'node', (e) => {
        const node = e.target;
        const label = node.data('label'), type = node.data('type');
        const specs = node.data('specializations'), locCount = node.data('locations_count');
        const gender = node.data('gender'), imgUrl = node.data('img_url');

        let html = '';
        if (type === 'doctor' && imgUrl) {
            html += `<div class="tt-photo"><img src="${esc(imgUrl)}" onerror="this.style.display='none'"><div>`;
        }
        html += `<div class="tt-name" style="color:${type === 'clinic' ? 'var(--accent)' : 'var(--green)'}">${esc(label)}</div>`;
        html += `<div class="tt-type">${type === 'clinic' ? 'Clinic' : 'Doctor'}`;
        if (type === 'clinic') {
            const dc = node.data('doctors_count');
            if (dc) html += ` &middot; ${dc} doctors`;
            const nip = node.data('nip');
            if (nip) html += ` &middot; NIP: ${nip}`;
        }
        if (type === 'doctor' && gender !== null && gender !== undefined) {
            html += ` &middot; <span class="tt-gender">${genderIcon(gender)} ${genderLabel(gender)}</span>`;
        }
        html += '</div>';
        if (locCount && locCount > 1) html += `<div class="tt-locations">${locCount} locations (merged)</div>`;
        if (specs) {
            html += '<div class="tt-specs">';
            for (const s of specs.split(', ').filter(Boolean)) html += `<span>${esc(s)}</span>`;
            html += '</div>';
        }
        if (type === 'doctor' && imgUrl) html += '</div></div>';
        tooltip.innerHTML = html;
        tooltip.style.display = 'block';
    });

    cy.on('mousemove', 'node', (e) => {
        tooltip.style.left = (e.originalEvent.clientX + 14) + 'px';
        tooltip.style.top = (e.originalEvent.clientY + 14) + 'px';
    });

    cy.on('mouseout', 'node', () => { tooltip.style.display = 'none'; });

    cy.on('mouseover', 'edge', (e) => {
        const br = e.target.data('booking_ratio');
        if (br == null) return;
        tooltip.innerHTML = `<div style="font-size:12px">Booking ratio: <b>${(br * 100).toFixed(0)}%</b></div>`;
        tooltip.style.display = 'block';
    });

    cy.on('mousemove', 'edge', (e) => {
        tooltip.style.left = (e.originalEvent.clientX + 14) + 'px';
        tooltip.style.top = (e.originalEvent.clientY + 14) + 'px';
    });

    cy.on('mouseout', 'edge', () => { tooltip.style.display = 'none'; });
}
