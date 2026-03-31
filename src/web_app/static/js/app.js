import { apiFetch, mergeParamFirst } from './utils.js';
import { initCy, clearGraph } from './graph.js';
import { setupSearch } from './search.js';
import { setupPathfinding } from './pathfind.js';
import { setupSpecSearch } from './spec-search.js';

// Shared mutable state — imported by all modules
export const state = {
    cy: null,
    expandedNodes: new Set(),
    specMatchDoctorIds: new Set(),
    motherNodeId: null,
    currentPaths: [],

    // Pathfinding
    fromId: null,
    fromType: 'clinic',
    toId: null,
    toType: 'clinic',
    pfMultiClinics: [],

    // Spec search
    ssFromId: null,
    ssFromType: 'clinic',
    ssSelectedSpecs: [],
};

function resetAll() {
    clearGraph();
    state.currentPaths = [];
    state.fromId = null; state.toId = null;
    state.fromType = 'clinic'; state.toType = 'clinic';
    state.expandedNodes.clear();
    state.motherNodeId = null;
    state.ssFromId = null;
    state.ssFromType = 'clinic';
    state.ssSelectedSpecs.length = 0;
    state.pfMultiClinics.length = 0;

    document.getElementById('ss-from').value = '';
    document.getElementById('ss-from').placeholder = 'Search clinic...';
    document.getElementById('ss-selected').innerHTML = '';
    document.getElementById('btn-spec-search').disabled = true;
    document.getElementById('spec-results').innerHTML = '';

    document.getElementById('pf-from').value = '';
    document.getElementById('pf-from').placeholder = 'Search clinic...';
    document.getElementById('pf-to').value = '';
    document.getElementById('pf-to').placeholder = 'Search clinic...';
    document.getElementById('pf-multi-from').value = '';
    document.getElementById('pf-multi-selected').innerHTML = '';
    document.getElementById('path-results').innerHTML = '';
    document.getElementById('btn-pathfind').disabled = true;

    document.getElementById('search-results').style.display = 'none';
    document.getElementById('search-box').value = '';

    document.getElementById('right-panel-content').innerHTML = `
        <div class="placeholder-msg">
            <div class="icon">&#128269;</div>
            <div>Search for a clinic or doctor to view details</div>
            <div style="font-size:11px;color:var(--text-dim)">Click = details &middot; Double-click = expand</div>
        </div>`;

    // Reset tab states
    document.querySelectorAll('.tab-switch').forEach(container => {
        const btns = container.querySelectorAll('.tab-btn');
        btns.forEach((b, i) => b.classList.toggle('active', i === 0));
    });
}

async function loadStats() {
    const data = await apiFetch(`/stats${mergeParamFirst()}`);
    const specInfo = data.doctors_with_specs ? ` \u00b7 ${data.doctors_with_specs.toLocaleString()} with specs` : '';
    document.getElementById('stats').textContent =
        `${data.clinics.toLocaleString()} clinics \u00b7 ${data.doctors.toLocaleString()} doctors \u00b7 ${data.edges.toLocaleString()} edges${specInfo}`;
}

document.addEventListener('DOMContentLoaded', () => {
    initCy();
    loadStats();
    setupSearch();
    setupPathfinding();
    setupSpecSearch();

    document.getElementById('btn-clear').addEventListener('click', resetAll);
    document.getElementById('chk-merge-nip').addEventListener('change', () => { resetAll(); loadStats(); });
});
