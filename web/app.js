/* global deck, maplibregl */
'use strict';

const REFRESH_MS = 5000;
const TRAIL_MINUTES = 5;   // son 5 dk: uçakların geçtiği hücreler iz olarak görünür

const ALERT_TYPES = {
  EMERGENCY_SQUAWK: { label: 'Acil squawk', color: [255, 77, 94] },
  RAPID_DESCENT:    { label: 'Ani alçalma', color: [255, 159, 67] },
  PROXIMITY:        { label: 'Yakın uçuş',  color: [34, 211, 238] },
  SPEED_JUMP:       { label: 'GPS sıçraması', color: [167, 139, 250] },
};

// Sıralı (sequential) yoğunluk rampası: 1, 2, 3, 4, 5+ uçak
const RAMP = [[64, 118, 196], [46, 164, 214], [80, 205, 170], [240, 205, 100], [245, 120, 70]];
const colorFor = (n) => RAMP[Math.max(0, Math.min(RAMP.length, n) - 1)];

const state = { cells: [], alerts: [], counts: {}, filter: null, selectedId: null };

// ---------- Harita ----------
const map = new maplibregl.Map({
  container: 'map',
  style: 'https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json',
  center: [35.2, 39.0],
  zoom: 5.3,
  // Kuşbakışı başlar; sağ tık / Ctrl+sürükle ile eğilince hücre sütunları 3B görünür
  pitch: 0,
  attributionControl: { compact: true },
});
map.addControl(new maplibregl.NavigationControl({ visualizePitch: true }), 'top-left');

// Masaüstünde sağdaki panelin kapladığı alanı hesaba kat ki Türkiye görünür alanın ortasında kalsın
const desktop = window.matchMedia('(min-width: 721px)');
const applyPadding = () => map.setPadding(desktop.matches ? { right: 392 } : { bottom: Math.round(window.innerHeight * 0.48) });
applyPadding();
desktop.addEventListener('change', applyPadding);

// interleaved: deck.gl katmanları MapLibre'nin WebGL bağlamına çizilir (tek canvas, ortak derinlik)
const overlay = new deck.MapboxOverlay({ interleaved: true, layers: [], getTooltip });
map.addControl(overlay);

function visibleAlerts() {
  return state.filter ? state.alerts.filter((a) => a.alert_type === state.filter) : state.alerts;
}

function renderLayers() {
  overlay.setProps({
    layers: [
      new deck.H3HexagonLayer({
        id: 'cells',
        data: state.cells,
        getHexagon: (d) => d.h3_cell,
        extruded: true,
        coverage: 0.92,
        getElevation: (d) => d.aircraft * 6000,
        getFillColor: (d) => [...colorFor(d.aircraft), 210],
        pickable: true,
        transitions: { getElevation: 600, getFillColor: 600 },
      }),
      new deck.ScatterplotLayer({
        id: 'alerts',
        data: visibleAlerts(),
        getPosition: (d) => [d.lon, d.lat],
        getFillColor: (d) => [...(ALERT_TYPES[d.alert_type]?.color || [255, 255, 255]), 235],
        getLineColor: [255, 255, 255, 220],
        stroked: true,
        lineWidthMinPixels: 1.5,
        radiusUnits: 'pixels',
        getRadius: (d) => (d.id === state.selectedId ? 11 : 6),
        pickable: true,
        // Uyarılar yükseltilmiş altıgenlerin arkasında kalmasın
        parameters: { depthCompare: 'always' },
        updateTriggers: { getRadius: state.selectedId },
      }),
    ],
  });
}

function getTooltip({ object, layer }) {
  if (!object) return null;
  if (layer.id === 'cells') {
    return {
      html: `<b>H3 ${object.h3_cell}</b><br>${object.aircraft} uçak`
        + `<br>Ort. irtifa: ${flightLevel(object.avg_altitude_m)}`
        + `<br>Ort. hız: ${knots(object.avg_speed_ms)}`,
    };
  }
  const t = ALERT_TYPES[object.alert_type];
  return {
    html: `<b>${t ? t.label : object.alert_type}</b> · ${esc(object.callsign || object.icao24)}`
      + `<br>${esc(object.details)}<br>${clock(object.event_time)}`,
  };
}

// ---------- Panel ----------
function renderStats(cellsResp, stats) {
  const cells = cellsResp.cells;
  document.getElementById('stat-cells').textContent = cells.length;
  document.getElementById('stat-peak').textContent = cells.reduce((m, c) => Math.max(m, c.aircraft), 0);
  const total = Object.values(stats.alerts_last_hour).reduce((s, n) => s + n, 0);
  document.getElementById('stat-alerts').textContent = total;
  document.getElementById('window').textContent = cellsResp.window_end
    ? `güncel: ${clock(cellsResp.window_end)}` : '—';
  renderSpark(stats.series.map((s) => s.active_cells));
}

function renderSpark(values) {
  const svg = document.getElementById('spark');
  if (values.length < 2) { svg.innerHTML = ''; return; }
  const w = 300, h = 44, pad = 3;
  const min = Math.min(...values), max = Math.max(...values);
  const span = max - min || 1;
  const pts = values.map((v, i) => [
    (i / (values.length - 1)) * w,
    h - pad - ((v - min) / span) * (h - 2 * pad),
  ]);
  const line = pts.map((p) => p.join(',')).join(' ');
  svg.innerHTML = `<polygon class="area" points="0,${h} ${line} ${w},${h}"></polygon>`
    + `<polyline class="line" points="${line}"></polyline>`;
}

function renderRamp() {
  document.getElementById('ramp').innerHTML = RAMP
    .map((c, i) => `<span style="background:rgb(${c.join(',')})">${i === RAMP.length - 1 ? `${i + 1}+` : i + 1}</span>`)
    .join('');
}

function renderChips() {
  const total = Object.values(state.counts).reduce((s, n) => s + n, 0);
  const chip = (key, label, color, n) => `<button class="chip ${state.filter === key ? 'active' : ''}" data-type="${key ?? ''}">`
    + (color ? `<span class="dot" style="background:rgb(${color.join(',')})"></span>` : '')
    + `${label} <span class="count">${n}</span></button>`;
  document.getElementById('chips').innerHTML = chip(null, 'Tümü', null, total)
    + Object.entries(ALERT_TYPES).map(([k, t]) => chip(k, t.label, t.color, state.counts[k] || 0)).join('');
}

function renderAlertList() {
  const list = document.getElementById('alert-list');
  const items = visibleAlerts();
  if (!items.length) {
    list.innerHTML = '<li class="empty">Son bir saatte uyarı yok.</li>';
    return;
  }
  list.innerHTML = items.map((a) => {
    const t = ALERT_TYPES[a.alert_type] || { label: a.alert_type, color: [255, 255, 255] };
    return `<li class="alert ${a.id === state.selectedId ? 'selected' : ''}" data-id="${a.id}">`
      + `<span class="dot" style="background:rgb(${t.color.join(',')})"></span>`
      + `<span class="title">${t.label} · ${esc(a.callsign || a.icao24)}</span>`
      + `<span class="time">${ago(a.event_time)}</span>`
      + `<span class="details">${esc(a.details)}</span></li>`;
  }).join('');
}

document.getElementById('chips').addEventListener('click', (e) => {
  const btn = e.target.closest('.chip');
  if (!btn) return;
  state.filter = btn.dataset.type || null;
  renderChips();
  renderAlertList();
  renderLayers();
});

document.getElementById('alert-list').addEventListener('click', (e) => {
  const li = e.target.closest('.alert');
  if (!li) return;
  const a = state.alerts.find((x) => String(x.id) === li.dataset.id);
  if (!a) return;
  state.selectedId = a.id;
  map.flyTo({ center: [a.lon, a.lat], zoom: 8, speed: 1.4 });
  renderAlertList();
  renderLayers();
});

// ---------- Veri ----------
async function getJson(path) {
  const res = await fetch(path);
  if (!res.ok) throw new Error(`${path}: ${res.status}`);
  return res.json();
}

async function refresh() {
  try {
    const [cells, alerts, stats] = await Promise.all([
      getJson(`/api/cells?minutes=${TRAIL_MINUTES}`),
      getJson('/api/alerts?limit=200&since_minutes=60'),
      getJson('/api/stats'),
    ]);
    state.cells = cells.cells;
    state.alerts = alerts;
    state.counts = stats.alerts_last_hour;
    renderStats(cells, stats);
    renderChips();
    renderAlertList();
    renderLayers();
    document.getElementById('live-dot').classList.remove('offline');
  } catch (err) {
    console.error(err);
    document.getElementById('live-dot').classList.add('offline');
  }
}

// ---------- Biçimlendirme ----------
function esc(s) {
  return String(s ?? '').replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
}
function clock(iso) {
  return new Date(iso).toLocaleTimeString('tr-TR', { hour: '2-digit', minute: '2-digit' });
}
function ago(iso) {
  const s = Math.max(0, Math.round((Date.now() - new Date(iso).getTime()) / 1000));
  if (s < 60) return `${s} sn önce`;
  if (s < 3600) return `${Math.floor(s / 60)} dk önce`;
  return `${Math.floor(s / 3600)} sa önce`;
}
function flightLevel(m) {
  return m == null ? '—' : `FL${String(Math.round((m * 3.28084) / 100)).padStart(3, '0')}`;
}
function knots(ms) {
  return ms == null ? '—' : `${Math.round(ms * 1.94384)} kt`;
}

// Panel, harita stilinin yüklenmesini beklemeden dolmaya başlar
renderRamp();
refresh();
setInterval(refresh, REFRESH_MS);
