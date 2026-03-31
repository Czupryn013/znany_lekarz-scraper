import { state } from './app.js';
import { truncateLabel, esc } from './utils.js';
import { loadNodeDetails } from './details.js';
import { loadNeighborhood } from './search.js';
import { setupTooltips } from './tooltip.js';

export const PATH_COLORS = ['#f59e0b','#ef4444','#16a34a','#3b82f6','#ec4899','#7c3aed','#06b6d4','#84cc16','#f97316','#6366f1'];

export function initCy() {
    state.cy = cytoscape({
        container: document.getElementById('cy'),
        style: [
            // Clinic base
            { selector: 'node[type="clinic"]', style: {
                'shape': 'round-rectangle',
                'background-color': '#dbeafe', 'background-opacity': 0.35,
                'label': 'data(short_label)', 'color': '#1e293b', 'font-size': '10px',
                'font-family': 'Inter, system-ui, sans-serif',
                'text-valign': 'bottom', 'text-margin-y': 5,
                'width': 30, 'height': 30,
                'border-width': 2, 'border-color': '#6b9fdb', 'border-opacity': 1,
                'text-max-width': '100px', 'text-wrap': 'ellipsis',
            }},
            // Expanded clinic
            { selector: 'node[type="clinic"].expanded', style: {
                'background-color': '#93b8e8', 'background-opacity': 1,
                'border-color': '#3b72c4', 'border-width': 2.5,
            }},
            // Doctor base
            { selector: 'node[type="doctor"]', style: {
                'shape': 'ellipse',
                'background-color': '#bbf7d0', 'background-opacity': 0.35,
                'label': 'data(short_label)', 'color': '#1e293b', 'font-size': '9px',
                'font-family': 'Inter, system-ui, sans-serif',
                'text-valign': 'bottom', 'text-margin-y': 4,
                'width': 18, 'height': 18,
                'border-width': 1.5, 'border-color': '#4aba6f', 'border-opacity': 1,
                'text-max-width': '80px', 'text-wrap': 'ellipsis',
            }},
            // Expanded doctor
            { selector: 'node[type="doctor"].expanded', style: {
                'background-color': '#6ee7a0', 'background-opacity': 1,
                'border-color': '#16a34a', 'border-width': 2.5,
                'width': 22, 'height': 22,
            }},
            // Doctor with image
            { selector: 'node[type="doctor"][?has_img]', style: {
                'background-image': 'data(img_url)', 'background-fit': 'cover',
                'background-clip': 'node', 'background-opacity': 1,
                'width': 26, 'height': 26,
            }},
            // Expanded doctor with image
            { selector: 'node[type="doctor"].expanded[?has_img]', style: {
                'border-color': '#16a34a', 'border-width': 3,
                'width': 28, 'height': 28,
            }},
            // Mother clinic
            { selector: 'node[type="clinic"].mother', style: {
                'width': 52, 'height': 52,
                'border-color': '#2563eb', 'border-width': 3.5, 'border-opacity': 1,
                'background-color': '#93b8e8', 'background-opacity': 1,
                'shadow-blur': 14, 'shadow-color': '#60a5fa', 'shadow-opacity': 0.6,
                'shadow-offset-x': 0, 'shadow-offset-y': 0,
                'font-size': '11px', 'font-weight': 'bold',
            }},
            // Mother doctor
            { selector: 'node[type="doctor"].mother', style: {
                'width': 44, 'height': 44,
                'border-color': '#f59e0b', 'border-width': 3.5, 'border-opacity': 1,
                'background-color': '#fde68a', 'background-opacity': 1,
                'shadow-blur': 14, 'shadow-color': '#f59e0b', 'shadow-opacity': 0.6,
                'shadow-offset-x': 0, 'shadow-offset-y': 0,
                'font-size': '11px', 'font-weight': 'bold',
            }},
            // Mother doctor with image
            { selector: 'node[type="doctor"].mother[?has_img]', style: {
                'width': 44, 'height': 44,
                'border-color': '#f59e0b', 'border-width': 4, 'border-opacity': 1,
                'background-opacity': 1,
                'shadow-blur': 14, 'shadow-color': '#f59e0b', 'shadow-opacity': 0.6,
                'shadow-offset-x': 0, 'shadow-offset-y': 0,
                'font-size': '11px', 'font-weight': 'bold',
            }},
            // Merged clinic
            { selector: 'node[type="clinic"][?is_merged]', style: {
                'content': 'data(merged_label)',
                'text-valign': 'bottom', 'text-margin-y': 5,
            }},
            // Degree classes
            { selector: 'node.degree-1', style: {
                'border-color': '#6b9fdb', 'border-width': 2, 'border-opacity': 1,
            }},
            { selector: 'node.degree-2', style: {
                'border-color': '#93c5fd', 'border-width': 1.5, 'border-opacity': 0.8,
            }},
            { selector: 'node.degree-3', style: {
                'border-color': '#bfdbfe', 'border-width': 1, 'border-opacity': 0.6,
            }},
            // Spec match doctor
            { selector: 'node[type="doctor"].spec-match', style: {
                'shadow-blur': 18, 'shadow-color': '#22c55e', 'shadow-opacity': 0.9,
                'shadow-offset-x': 0, 'shadow-offset-y': 0,
                'border-color': '#eab308', 'border-width': 3, 'border-opacity': 1,
                'background-color': '#86efac', 'background-opacity': 1,
            }},
            { selector: 'node[type="doctor"].spec-match[?has_img]', style: {
                'shadow-blur': 18, 'shadow-color': '#22c55e', 'shadow-opacity': 0.9,
                'border-color': '#eab308', 'border-width': 3.5,
            }},
            // Spec match clinic
            { selector: 'node[type="clinic"].spec-match', style: {
                'shadow-blur': 18, 'shadow-color': '#22c55e', 'shadow-opacity': 0.9,
                'shadow-offset-x': 0, 'shadow-offset-y': 0,
            }},
            // Edges
            { selector: 'edge', style: {
                'line-color': '#9ca3af', 'width': 1.2, 'curve-style': 'bezier', 'opacity': 0.7,
            }},
            { selector: 'edge[booking_ratio]', style: {
                'line-color': 'mapData(booking_ratio, 0, 1, #9ca3af, #3b82f6)',
                'width': 'mapData(booking_ratio, 0, 1, 1.2, 2.5)',
            }},
            // Path highlight / dim
            { selector: '.path-highlight', style: { 'opacity': 1, 'z-index': 10 }},
            { selector: '.dimmed', style: { 'opacity': 0.12 }},
        ],
        layout: { name: 'preset' },
        minZoom: 0.1, maxZoom: 5,
    });

    const cy = state.cy;

    // Single click → show details
    let tapTimer = null;
    cy.on('tap', 'node', (e) => {
        if (tapTimer) return;
        const node = e.target;
        tapTimer = setTimeout(async () => {
            tapTimer = null;
            await loadNodeDetails(node.data('type'), node.data('raw_id'));
        }, 250);
    });

    // Double click → expand
    cy.on('dbltap', 'node', async (e) => {
        if (tapTimer) { clearTimeout(tapTimer); tapTimer = null; }
        const node = e.target;
        const nodeId = node.id();
        state.expandedNodes.add(nodeId);
        node.addClass('expanded');
        loadNodeDetails(node.data('type'), node.data('raw_id'));
        await loadNeighborhood(node.data('type'), node.data('raw_id'), 1);
    });

    // Compound drag: clinic drags its exclusive doctors
    let dragState = null;
    cy.on('grab', 'node[type="clinic"]', (e) => {
        const clinic = e.target;
        const clinicPos = clinic.position();
        const followers = [];
        clinic.connectedEdges().connectedNodes().forEach(n => {
            if (n.id() === clinic.id()) return;
            if (n.data('type') !== 'doctor') return;
            const clinicNeighbors = n.connectedEdges().connectedNodes().filter(nn => nn.data('type') === 'clinic');
            if (clinicNeighbors.length === 1) {
                const pos = n.position();
                followers.push({ node: n, dx: pos.x - clinicPos.x, dy: pos.y - clinicPos.y });
            }
        });
        dragState = { clinic, initPos: { ...clinicPos }, followers };
    });

    cy.on('drag', 'node[type="clinic"]', (e) => {
        if (!dragState || dragState.clinic.id() !== e.target.id()) return;
        const pos = e.target.position();
        for (const f of dragState.followers) {
            f.node.position({ x: pos.x + f.dx, y: pos.y + f.dy });
        }
    });

    cy.on('free', 'node[type="clinic"]', () => { dragState = null; });

    setupTooltips();
}

export function addToGraph(nodes, edges) {
    const cy = state.cy;
    const elements = [];
    for (const n of nodes) {
        if (!cy.getElementById(n.id).length) {
            const shortLbl = truncateLabel(n.label || n.id, 22);
            const locCount = n.locations_count || 0;
            const d = {
                id: n.id, label: n.label || n.id, short_label: shortLbl,
                type: n.type, raw_id: n.raw_id, doctors_count: n.doctors_count || 1,
                specializations: (n.specializations || []).join(', '), locations_count: locCount,
            };
            if (n.type === 'clinic' && locCount > 1) {
                d.is_merged = true;
                d.merged_label = '\uD83D\uDCCD ' + shortLbl;
            }
            if (n.type === 'doctor') {
                d.gender = n.gender;
                if (n.img_url) { d.img_url = n.img_url; d.has_img = true; }
            }
            elements.push({ group: 'nodes', data: d });
        }
    }
    for (const e of edges) {
        const pair = [e.source, e.target].sort();
        const edgeId = `${pair[0]}-${pair[1]}`;
        if (!cy.getElementById(edgeId).length) {
            const d = { id: edgeId, source: e.source, target: e.target };
            if (e.booking_ratio != null) d.booking_ratio = e.booking_ratio;
            elements.push({ group: 'edges', data: d });
        }
    }
    if (elements.length) {
        const existingNodeIds = new Set();
        cy.nodes().forEach(n => existingNodeIds.add(n.id()));

        cy.add(elements);

        // Apply expanded class
        for (const nodeId of state.expandedNodes) {
            const n = cy.getElementById(nodeId);
            if (n.length) n.addClass('expanded');
        }

        // Re-apply spec-match
        if (state.specMatchDoctorIds.size) {
            cy.nodes().forEach(n => {
                if (state.specMatchDoctorIds.has(n.id())) {
                    n.addClass('spec-match');
                    n.connectedEdges().connectedNodes().forEach(nb => {
                        if (nb.data('type') === 'clinic') nb.addClass('spec-match');
                    });
                }
            });
        }

        applyDegreeClasses();

        // Lock existing nodes for incremental layout
        const isIncremental = existingNodeIds.size > 0;
        if (isIncremental) {
            existingNodeIds.forEach(id => {
                const n = cy.getElementById(id);
                if (n.length) n.lock();
            });
        }

        const layoutObj = cy.layout({
            name: 'cose-bilkent', animate: true, animationDuration: 600,
            nodeRepulsion: isIncremental ? 18000 : 25000,
            idealEdgeLength: isIncremental ? 120 : 140,
            edgeElasticity: 0.05,
            gravity: isIncremental ? 0.15 : 0.08,
            gravityRange: 1.5, numIter: 2500, tile: true,
            fit: !isIncremental, padding: 50,
            nestingFactor: 0.1, tilingPaddingVertical: 20, tilingPaddingHorizontal: 20,
        });

        if (isIncremental) {
            layoutObj.on('layoutstop', () => {
                existingNodeIds.forEach(id => {
                    const n = cy.getElementById(id);
                    if (n.length) n.unlock();
                });
            });
        }

        layoutObj.run();
    }
}

export function applyDegreeClasses() {
    const cy = state.cy;
    if (!state.motherNodeId) return;
    const mother = cy.getElementById(state.motherNodeId);
    if (!mother.length) return;

    cy.nodes().removeClass('mother degree-1 degree-2 degree-3');

    const visited = new Set();
    const clinicDist = new Map();
    const queue = [{ id: state.motherNodeId, dist: 0 }];
    visited.add(state.motherNodeId);

    // If mother is a doctor, mark it and start BFS from its clinics at dist 0
    const motherType = mother.data('type');
    if (motherType === 'doctor') {
        mother.addClass('mother');
        mother.connectedEdges().connectedNodes().forEach(neighbor => {
            const nid = neighbor.id();
            if (neighbor.data('type') === 'clinic' && !visited.has(nid)) {
                visited.add(nid);
                clinicDist.set(nid, 0);
                queue.push({ id: nid, dist: 0 });
            }
        });
    } else {
        clinicDist.set(state.motherNodeId, 0);
    }

    while (queue.length) {
        const { id, dist } = queue.shift();
        const node = cy.getElementById(id);
        if (!node.length) continue;

        node.connectedEdges().connectedNodes().forEach(neighbor => {
            const nid = neighbor.id();
            if (visited.has(nid)) return;
            visited.add(nid);

            const ntype = neighbor.data('type');
            if (ntype === 'clinic') {
                clinicDist.set(nid, dist + 1);
                queue.push({ id: nid, dist: dist + 1 });
            } else {
                queue.push({ id: nid, dist });
            }
        });
    }

    for (const [nodeId, dist] of clinicDist) {
        const node = cy.getElementById(nodeId);
        if (!node.length || node.data('type') !== 'clinic') continue;
        if (dist === 0) node.addClass('mother');
        else if (dist === 1) node.addClass('degree-1');
        else if (dist === 2) node.addClass('degree-2');
        else node.addClass('degree-3');
    }
}

export function clearGraph() {
    state.cy.elements().remove();
    state.specMatchDoctorIds.clear();
}

export function highlightPath(idx) {
    const cy = state.cy;
    cy.elements().removeClass('path-highlight dimmed');
    document.querySelectorAll('.path-item').forEach(el => el.classList.remove('active'));
    if (idx < 0 || idx >= state.currentPaths.length) return;
    const path = state.currentPaths[idx], color = PATH_COLORS[idx % PATH_COLORS.length];
    cy.elements().addClass('dimmed');
    for (let i = 0; i < path.length; i++) {
        const node = cy.getElementById(path[i].id);
        node.removeClass('dimmed').addClass('path-highlight').style('border-color', color).style('border-width', 2.5);
        if (i < path.length - 1) {
            const pair = [path[i].id, path[i+1].id].sort();
            const edge = cy.getElementById(`${pair[0]}-${pair[1]}`);
            if (edge.length) edge.removeClass('dimmed').addClass('path-highlight').style('line-color', color).style('width', 2.5);
        }
    }
    const items = document.querySelectorAll('.path-item');
    if (items[idx]) items[idx].classList.add('active');
}
