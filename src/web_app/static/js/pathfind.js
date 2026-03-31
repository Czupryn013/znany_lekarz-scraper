import { state } from './app.js';
import { apiFetch, mergeParam, esc } from './utils.js';
import { clearGraph, addToGraph, highlightPath, PATH_COLORS } from './graph.js';
import { setupNodeAutocomplete, setupTabSwitch, setActiveTab } from './autocomplete.js';

export function setupPathfinding() {
    // Tab switchers for From and To
    setupTabSwitch('pf-from-tabs', (type) => {
        state.fromType = type;
        state.fromId = null;
        const input = document.getElementById('pf-from');
        input.value = '';
        input.placeholder = type === 'clinic' ? 'Search clinic...' : 'Search doctor...';
        // Hide multi-mode when doctor is selected
        const multiLabel = document.getElementById('pf-multi-label');
        if (multiLabel) multiLabel.style.display = type === 'doctor' ? 'none' : '';
        if (type === 'doctor' && document.getElementById('pf-multi-mode').checked) {
            document.getElementById('pf-multi-mode').checked = false;
            togglePfMultiMode(false);
        }
        localStorage.removeItem('pf_from');
        updatePfBtn();
    });

    setupTabSwitch('pf-to-tabs', (type) => {
        state.toType = type;
        state.toId = null;
        const input = document.getElementById('pf-to');
        input.value = '';
        input.placeholder = type === 'clinic' ? 'Search clinic...' : 'Search doctor...';
        localStorage.removeItem('pf_to');
        updatePfBtn();
    });

    // Autocomplete for From (single)
    setupNodeAutocomplete(
        document.getElementById('pf-from'),
        document.getElementById('ac-from'),
        () => state.fromType,
        (id, name) => {
            state.fromId = id;
            localStorage.setItem('pf_from', JSON.stringify({ id, name, type: state.fromType }));
            updatePfBtn();
        }
    );

    // Autocomplete for To
    setupNodeAutocomplete(
        document.getElementById('pf-to'),
        document.getElementById('ac-to'),
        () => state.toType,
        (id, name) => {
            state.toId = id;
            localStorage.setItem('pf_to', JSON.stringify({ id, name, type: state.toType }));
            updatePfBtn();
        }
    );

    // Multi-clinic "from"
    setupNodeAutocomplete(
        document.getElementById('pf-multi-from'),
        document.getElementById('ac-pf-multi-from'),
        () => 'clinic',
        (id, name) => {
            if (!state.pfMultiClinics.some(c => c.id === id)) {
                state.pfMultiClinics.push({ id, name });
                localStorage.setItem('pf_multi_clinics', JSON.stringify(state.pfMultiClinics));
                renderPfMultiClinics();
                updatePfBtn();
            }
            document.getElementById('pf-multi-from').value = '';
        }
    );

    // Multi-mode toggle
    document.getElementById('pf-multi-mode').addEventListener('change', (e) => {
        const multi = e.target.checked;
        localStorage.setItem('pf_multi_mode', multi);
        togglePfMultiMode(multi);
    });

    // Find paths button
    document.getElementById('btn-pathfind').addEventListener('click', doPathfind);

    // Clear button
    document.getElementById('btn-pf-clear').addEventListener('click', () => {
        state.fromId = null; state.toId = null;
        state.fromType = 'clinic'; state.toType = 'clinic';
        setActiveTab('pf-from-tabs', 'clinic');
        setActiveTab('pf-to-tabs', 'clinic');
        document.getElementById('pf-from').value = '';
        document.getElementById('pf-from').placeholder = 'Search clinic...';
        document.getElementById('pf-to').value = '';
        document.getElementById('pf-to').placeholder = 'Search clinic...';
        state.pfMultiClinics.length = 0;
        renderPfMultiClinics();
        document.getElementById('pf-multi-from').value = '';
        document.getElementById('path-results').innerHTML = '';
        document.getElementById('btn-pathfind').disabled = true;
        state.currentPaths = [];
        const multiLabel = document.getElementById('pf-multi-label');
        if (multiLabel) multiLabel.style.display = '';
        document.getElementById('pf-multi-mode').checked = false;
        togglePfMultiMode(false);
        localStorage.removeItem('pf_from');
        localStorage.removeItem('pf_to');
        localStorage.removeItem('pf_multi_clinics');
        localStorage.removeItem('pf_multi_mode');
    });

    // Restore from localStorage
    restorePathfindState();
}

function togglePfMultiMode(multi) {
    document.getElementById('pf-single-from').style.display = multi ? 'none' : '';
    document.getElementById('pf-multi-container').style.display = multi ? '' : 'none';
    updatePfBtn();
}

function renderPfMultiClinics() {
    const el = document.getElementById('pf-multi-selected');
    el.innerHTML = '';
    for (const c of state.pfMultiClinics) {
        const tag = document.createElement('span');
        tag.className = 'selected-spec';
        tag.innerHTML = `${esc(c.name)} <span class="remove-spec">\u00d7</span>`;
        tag.querySelector('.remove-spec').onclick = () => {
            const i = state.pfMultiClinics.findIndex(x => x.id === c.id);
            if (i >= 0) state.pfMultiClinics.splice(i, 1);
            localStorage.setItem('pf_multi_clinics', JSON.stringify(state.pfMultiClinics));
            renderPfMultiClinics();
            updatePfBtn();
        };
        el.appendChild(tag);
    }
}

function updatePfBtn() {
    const multi = document.getElementById('pf-multi-mode').checked;
    const hasFrom = multi ? state.pfMultiClinics.length > 0 : state.fromId !== null;
    document.getElementById('btn-pathfind').disabled = !(hasFrom && state.toId);
}

async function doPathfind() {
    const multi = document.getElementById('pf-multi-mode').checked;
    const btn = document.getElementById('btn-pathfind');
    const pr = document.getElementById('path-results');

    // Build from param
    let fromParam;
    if (multi) {
        fromParam = `from_clinic=${state.pfMultiClinics.map(c => c.id).join(',')}`;
    } else if (state.fromType === 'doctor') {
        fromParam = `from_doctor=${state.fromId}`;
    } else {
        fromParam = `from_clinic=${state.fromId}`;
    }

    // Build to param
    let toParam;
    if (state.toType === 'doctor') {
        toParam = `to_doctor=${state.toId}`;
    } else {
        toParam = `to_clinic=${state.toId}`;
    }

    if (!fromParam || !state.toId) return;
    btn.disabled = true; btn.textContent = 'Searching...';
    const data = await apiFetch(`/pathfind?${fromParam}&${toParam}&k=${document.getElementById('pf-k').value}${mergeParam()}`);
    btn.disabled = false; btn.textContent = 'Find Paths';
    state.currentPaths = data.paths || [];

    if (data.error) { pr.innerHTML = `<div style="color:var(--red);font-size:13px;padding:8px">${esc(data.error)}</div>`; return; }
    if (!state.currentPaths.length) { pr.innerHTML = '<div style="color:var(--text-dim);font-size:13px;padding:8px">No paths found.</div>'; return; }

    clearGraph();
    const allN = [], allE = [], seen = new Set();
    for (const path of state.currentPaths) {
        for (let i = 0; i < path.length; i++) {
            const n = path[i];
            if (!seen.has(n.id)) { seen.add(n.id); allN.push({ id: n.id, type: n.type, raw_id: n.raw_id, label: n.label, doctors_count: 1, specializations: n.specializations || [] }); }
            if (i < path.length - 1) allE.push({ source: n.id, target: path[i+1].id });
        }
    }
    if (state.currentPaths.length && state.currentPaths[0].length) {
        state.motherNodeId = state.currentPaths[0][0].id;
    }
    addToGraph(allN, allE);

    pr.innerHTML = `<div style="font-size:12px;color:var(--text-dim);margin-bottom:6px">${state.currentPaths.length} path(s)</div>`;
    state.currentPaths.forEach((path, idx) => {
        const el = document.createElement('div');
        el.className = 'path-item';
        el.style.borderLeft = `3px solid ${PATH_COLORS[idx % PATH_COLORS.length]}`;
        el.innerHTML = `<div style="font-size:11px;color:var(--text-dim);margin-bottom:3px">Path ${idx+1} &middot; ${Math.floor(path.length/2)} hops</div>`
            + path.map(n => `<span class="path-node path-node-${n.type}">${esc(n.label)}</span>`).join('<span class="path-arrow">\u2192</span>');
        el.onclick = () => highlightPath(idx);
        pr.appendChild(el);
    });
    highlightPath(0);
}

function restorePathfindState() {
    // Restore from type
    const savedFrom = JSON.parse(localStorage.getItem('pf_from') || 'null');
    if (savedFrom) {
        state.fromId = savedFrom.id;
        state.fromType = savedFrom.type || 'clinic';
        document.getElementById('pf-from').value = savedFrom.name;
        document.getElementById('pf-from').placeholder = state.fromType === 'clinic' ? 'Search clinic...' : 'Search doctor...';
        setActiveTab('pf-from-tabs', state.fromType);
        if (state.fromType === 'doctor') {
            const multiLabel = document.getElementById('pf-multi-label');
            if (multiLabel) multiLabel.style.display = 'none';
        }
    }

    // Restore to type
    const savedTo = JSON.parse(localStorage.getItem('pf_to') || 'null');
    if (savedTo) {
        state.toId = savedTo.id;
        state.toType = savedTo.type || 'clinic';
        document.getElementById('pf-to').value = savedTo.name;
        document.getElementById('pf-to').placeholder = state.toType === 'clinic' ? 'Search clinic...' : 'Search doctor...';
        setActiveTab('pf-to-tabs', state.toType);
    }

    // Restore multi mode
    const savedMultiMode = localStorage.getItem('pf_multi_mode') === 'true';
    if (savedMultiMode && state.fromType === 'clinic') {
        document.getElementById('pf-multi-mode').checked = true;
        togglePfMultiMode(true);
    }
    const savedMulti = JSON.parse(localStorage.getItem('pf_multi_clinics') || '[]');
    if (savedMulti.length) {
        state.pfMultiClinics.push(...savedMulti);
        renderPfMultiClinics();
    }

    updatePfBtn();
}
