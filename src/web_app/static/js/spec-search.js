import { state } from './app.js';
import { apiFetch, mergeParam, esc } from './utils.js';
import { clearGraph, addToGraph } from './graph.js';
import { loadNodeDetails } from './details.js';
import { setupNodeAutocomplete, setupTabSwitch, setActiveTab } from './autocomplete.js';

export function setupSpecSearch() {
    // Tab switcher for From
    setupTabSwitch('ss-from-tabs', (type) => {
        state.ssFromType = type;
        state.ssFromId = null;
        const input = document.getElementById('ss-from');
        input.value = '';
        input.placeholder = type === 'clinic' ? 'Search clinic...' : 'Search doctor...';
        localStorage.removeItem('ss_from');
        updateSsBtn();
    });

    // Autocomplete for From
    setupNodeAutocomplete(
        document.getElementById('ss-from'),
        document.getElementById('ac-ss-from'),
        () => state.ssFromType,
        (id, name) => {
            state.ssFromId = id;
            localStorage.setItem('ss_from', JSON.stringify({ id, name, type: state.ssFromType }));
            updateSsBtn();
        }
    );

    // Specialization autocomplete
    const ssSpecInput = document.getElementById('ss-spec');
    const acSsSpec = document.getElementById('ac-ss-spec');
    let specTimeout;

    ssSpecInput.addEventListener('input', () => {
        clearTimeout(specTimeout);
        const q = ssSpecInput.value.trim();
        if (q.length < 2) { acSsSpec.style.display = 'none'; return; }
        specTimeout = setTimeout(async () => {
            const data = await apiFetch(`/search-specializations?q=${encodeURIComponent(q)}`);
            acSsSpec.innerHTML = '';
            const already = new Set(state.ssSelectedSpecs.map(s => s.id));
            for (const s of data) {
                if (already.has(s.id)) continue;
                const el = document.createElement('div');
                el.className = 'autocomplete-item';
                el.innerHTML = `${esc(s.name)} <span style="color:var(--text-dim);font-size:11px">${s.doctor_count} doctors</span>`;
                el.onclick = () => {
                    state.ssSelectedSpecs.push({ id: s.id, name: s.name });
                    ssSpecInput.value = ''; acSsSpec.style.display = 'none';
                    renderSelectedSpecs(); updateSsBtn();
                };
                acSsSpec.appendChild(el);
            }
            acSsSpec.style.display = data.length ? 'block' : 'none';
        }, 300);
    });
    ssSpecInput.addEventListener('blur', () => setTimeout(() => acSsSpec.style.display = 'none', 200));

    // Find button
    document.getElementById('btn-spec-search').addEventListener('click', doSpecSearch);

    // Clear button
    document.getElementById('btn-ss-clear').addEventListener('click', () => {
        state.ssFromId = null;
        state.ssFromType = 'clinic';
        setActiveTab('ss-from-tabs', 'clinic');
        document.getElementById('ss-from').value = '';
        document.getElementById('ss-from').placeholder = 'Search clinic...';
        state.ssSelectedSpecs.length = 0;
        renderSelectedSpecs();
        document.getElementById('ss-hops').value = '3';
        document.getElementById('spec-results').innerHTML = '';
        document.getElementById('btn-spec-search').disabled = true;
        localStorage.removeItem('ss_from');
    });

    // Restore state
    restoreSpecSearchState();
}

function renderSelectedSpecs() {
    const el = document.getElementById('ss-selected');
    el.innerHTML = '';
    for (const s of state.ssSelectedSpecs) {
        const tag = document.createElement('span');
        tag.className = 'selected-spec';
        tag.innerHTML = `${esc(s.name)} <span class="remove-spec">\u00d7</span>`;
        tag.querySelector('.remove-spec').onclick = () => {
            const i = state.ssSelectedSpecs.findIndex(x => x.id === s.id);
            if (i >= 0) state.ssSelectedSpecs.splice(i, 1);
            renderSelectedSpecs(); updateSsBtn();
        };
        el.appendChild(tag);
    }
}

function updateSsBtn() {
    document.getElementById('btn-spec-search').disabled = !(state.ssFromId && state.ssSelectedSpecs.length);
}

async function doSpecSearch() {
    if (!state.ssFromId || !state.ssSelectedSpecs.length) return;
    const btn = document.getElementById('btn-spec-search');
    btn.disabled = true; btn.textContent = 'Searching...';

    const hops = document.getElementById('ss-hops').value;
    const specIds = state.ssSelectedSpecs.map(s => s.id).join(',');

    const fromParam = state.ssFromType === 'clinic'
        ? `from_clinic=${state.ssFromId}`
        : `from_doctor=${state.ssFromId}`;

    const data = await apiFetch(`/find-by-specialization?${fromParam}&spec_ids=${specIds}&hops=${hops}${mergeParam()}`);
    btn.disabled = false; btn.textContent = 'Find Doctors';

    const sr = document.getElementById('spec-results');
    if (data.error) { sr.innerHTML = `<div style="color:var(--red);font-size:13px;padding:8px">${esc(data.error)}</div>`; return; }

    clearGraph();
    state.expandedNodes.clear();

    // Set mother node based on from type
    if (state.ssFromType === 'clinic') {
        state.motherNodeId = `c_${state.ssFromId}`;
    } else {
        state.motherNodeId = `d_${state.ssFromId}`;
    }
    state.expandedNodes.add(state.motherNodeId);
    addToGraph(data.nodes, data.edges);

    // Mark matched doctors
    state.specMatchDoctorIds.clear();
    data.results.forEach(r => state.specMatchDoctorIds.add(`d_${r.doctor_id}`));
    const matchedIds = state.specMatchDoctorIds;
    const specMatchClinicIds = new Set();
    state.cy.nodes().forEach(n => {
        if (matchedIds.has(n.id())) {
            n.addClass('spec-match');
            n.connectedEdges().connectedNodes().forEach(neighbor => {
                if (neighbor.data('type') === 'clinic') specMatchClinicIds.add(neighbor.id());
            });
        }
    });
    specMatchClinicIds.forEach(cid => {
        const cn = state.cy.getElementById(cid);
        if (cn.length) cn.addClass('spec-match');
    });

    for (const nodeId of state.expandedNodes) {
        const n = state.cy.getElementById(nodeId);
        if (n.length) n.addClass('expanded');
    }

    if (!data.results.length) { sr.innerHTML = '<div style="color:var(--text-dim);font-size:13px;padding:8px">No doctors found in range.</div>'; return; }

    let html = `<div style="font-size:12px;color:var(--text-dim);margin-bottom:6px">${data.total_found} doctor(s) found</div>`;
    for (const r of data.results) {
        const dn = data.nodes.find(n => n.raw_id === r.doctor_id && n.type === 'doctor');
        const label = dn ? dn.label : `Doctor ${r.doctor_id}`;
        const clinicsAway = Math.floor((r.hops - 1) / 2);
        const distLabel = clinicsAway === 0 ? 'direct' : `${clinicsAway} clinic${clinicsAway > 1 ? 's' : ''} away`;
        html += `<div class="spec-result-item" data-id="d_${r.doctor_id}">`;
        html += `<span><span class="badge badge-doctor">D</span> ${esc(label)} <span style="color:var(--purple);font-size:11px">${esc((r.matching_specs||[]).join(', '))}</span></span>`;
        html += `<span class="hops">${distLabel}</span>`;
        html += '</div>';
    }
    sr.innerHTML = html;
    sr.querySelectorAll('.spec-result-item').forEach(el => {
        el.onclick = () => {
            const n = state.cy.getElementById(el.dataset.id);
            if (n.length) {
                state.cy.animate({ center: { eles: n }, zoom: 2 }, { duration: 300 });
                n.select();
                loadNodeDetails('doctor', parseInt(el.dataset.id.split('_')[1]));
            }
        };
    });
}

function restoreSpecSearchState() {
    const savedSs = JSON.parse(localStorage.getItem('ss_from') || 'null');
    if (savedSs) {
        state.ssFromId = savedSs.id;
        state.ssFromType = savedSs.type || 'clinic';
        document.getElementById('ss-from').value = savedSs.name;
        document.getElementById('ss-from').placeholder = state.ssFromType === 'clinic' ? 'Search clinic...' : 'Search doctor...';
        setActiveTab('ss-from-tabs', state.ssFromType);
        updateSsBtn();
    }
}
