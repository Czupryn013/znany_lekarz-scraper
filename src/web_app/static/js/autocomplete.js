import { apiFetch, esc } from './utils.js';

/**
 * Set up a generic node autocomplete that searches clinics or doctors based on getNodeType().
 * @param {HTMLInputElement} input
 * @param {HTMLElement} dropdown
 * @param {() => 'clinic' | 'doctor'} getNodeType
 * @param {(id: number, name: string) => void} onSelect
 */
export function setupNodeAutocomplete(input, dropdown, getNodeType, onSelect) {
    let t;
    input.addEventListener('input', () => {
        clearTimeout(t);
        const q = input.value.trim();
        if (q.length < 2) { dropdown.style.display = 'none'; return; }
        t = setTimeout(async () => {
            const data = await apiFetch(`/search?q=${encodeURIComponent(q)}`);
            dropdown.innerHTML = '';
            const nodeType = getNodeType();

            if (nodeType === 'clinic') {
                for (const c of data.clinics) {
                    const el = document.createElement('div');
                    el.className = 'autocomplete-item';
                    el.innerHTML = `<div>${esc(c.name)}</div>${c.address ? `<div class="ac-address">${esc(c.address)}</div>` : ''}`;
                    el.onclick = () => { input.value = c.name; dropdown.style.display = 'none'; onSelect(c.id, c.name); };
                    dropdown.appendChild(el);
                }
                dropdown.style.display = data.clinics.length ? 'block' : 'none';
            } else {
                for (const d of data.doctors) {
                    const el = document.createElement('div');
                    el.className = 'autocomplete-item';
                    el.textContent = `${d.name} ${d.surname}`;
                    el.onclick = () => {
                        const fullName = `${d.name} ${d.surname}`;
                        input.value = fullName;
                        dropdown.style.display = 'none';
                        onSelect(d.id, fullName);
                    };
                    dropdown.appendChild(el);
                }
                dropdown.style.display = data.doctors.length ? 'block' : 'none';
            }
        }, 300);
    });
    input.addEventListener('blur', () => setTimeout(() => dropdown.style.display = 'none', 200));
}

/**
 * Wire up a tab-switch container so clicking buttons toggles .active and calls onSwitch.
 * @param {string} containerId
 * @param {(value: string) => void} onSwitch
 */
export function setupTabSwitch(containerId, onSwitch) {
    const container = document.getElementById(containerId);
    if (!container) return;
    const btns = container.querySelectorAll('.tab-btn');
    btns.forEach(btn => {
        btn.addEventListener('click', () => {
            btns.forEach(b => b.classList.remove('active'));
            btn.classList.add('active');
            onSwitch(btn.dataset.value);
        });
    });
}

/**
 * Set the active tab button programmatically (for localStorage restore).
 */
export function setActiveTab(containerId, value) {
    const container = document.getElementById(containerId);
    if (!container) return;
    const btns = container.querySelectorAll('.tab-btn');
    btns.forEach(b => {
        b.classList.toggle('active', b.dataset.value === value);
    });
}
