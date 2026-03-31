import { state } from './app.js';
import { apiFetch, mergeParam, showLoading, esc } from './utils.js';
import { addToGraph } from './graph.js';
import { loadNodeDetails } from './details.js';

export async function loadNeighborhood(type, id, depth) {
    showLoading(true);
    const data = await apiFetch(`/neighbors/${type}/${id}?depth=${depth}${mergeParam()}`);
    addToGraph(data.nodes, data.edges);
    showLoading(false);
}

export function setupSearch() {
    let searchTimeout;
    const searchBox = document.getElementById('search-box');
    const searchResults = document.getElementById('search-results');
    const searchList = document.getElementById('search-list');

    searchBox.addEventListener('input', () => {
        clearTimeout(searchTimeout);
        const q = searchBox.value.trim();
        if (q.length < 2) { searchResults.style.display = 'none'; return; }
        searchTimeout = setTimeout(() => doSearch(q), 300);
    });

    async function doSearch(q) {
        const data = await apiFetch(`/search?q=${encodeURIComponent(q)}`);
        searchList.innerHTML = '';
        searchResults.style.display = 'block';
        for (const c of data.clinics) {
            const el = document.createElement('div');
            el.className = 'search-item';
            el.innerHTML = `<span class="badge badge-clinic">C</span> <div><div>${esc(c.name)}</div>${c.address ? `<div class="ac-address">${esc(c.address)}</div>` : ''}</div>`;
            el.onclick = () => {
                const nodeId = `c_${c.id}`;
                if (!state.motherNodeId) state.motherNodeId = nodeId;
                state.expandedNodes.add(nodeId);
                loadNeighborhood('clinic', c.id, 1);
                loadNodeDetails('clinic', c.id);
            };
            searchList.appendChild(el);
        }
        for (const d of data.doctors) {
            const el = document.createElement('div');
            el.className = 'search-item';
            el.innerHTML = `<span class="badge badge-doctor">D</span> ${esc(d.name)} ${esc(d.surname)}`;
            el.onclick = () => {
                const nodeId = `d_${d.id}`;
                if (!state.motherNodeId) state.motherNodeId = nodeId;
                state.expandedNodes.add(nodeId);
                loadNeighborhood('doctor', d.id, 1);
                loadNodeDetails('doctor', d.id);
            };
            searchList.appendChild(el);
        }
        if (!data.clinics.length && !data.doctors.length)
            searchList.innerHTML = '<div style="padding:8px;color:var(--text-dim);font-size:13px">No results</div>';
    }
}
