const API = '/api';

export function esc(s) {
    if (!s) return '';
    const d = document.createElement('div');
    d.textContent = s;
    return d.innerHTML;
}

export function showLoading(show) {
    document.getElementById('loading').style.display = show ? 'block' : 'none';
}

export function truncateLabel(s, max) {
    return !s ? '' : s.length > max ? s.substring(0, max) + '\u2026' : s;
}

export function genderLabel(g) {
    return g === 1 ? 'Male' : g === 0 ? 'Female' : null;
}

export function genderIcon(g) {
    return g === 1 ? '\u2642' : g === 0 ? '\u2640' : '';
}

export function isMergeNip() {
    return document.getElementById('chk-merge-nip').checked;
}

export function mergeParam() {
    return isMergeNip() ? '&merge_nip=true' : '';
}

export function mergeParamFirst() {
    return isMergeNip() ? '?merge_nip=true' : '';
}

export async function apiFetch(path) {
    return (await fetch(`${API}${path}`)).json();
}
