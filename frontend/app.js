/**
 * app.js — Electric Management Dashboard
 * Vanilla JS, ~150 dòng, không cần build.
 * Cần: frontend/.env.js (xem bên dưới) với FIREBASE_CONFIG.
 */

import { initializeApp } from 'https://www.gstatic.com/firebasejs/11.6.0/firebase-app.js';
import { getDatabase, ref, onValue } from 'https://www.gstatic.com/firebasejs/11.6.0/firebase-database.js';

// ── Config ──────────────────────────────────────────────────────────────────
// Đặt config Firebase của bạn vào đây (lấy từ Firebase Console → Project settings)
import { FIREBASE_CONFIG } from './config.js';

// ── Init Firebase ────────────────────────────────────────────────────────────
const app = initializeApp(FIREBASE_CONFIG);
const db  = getDatabase(app);

// ── DOM helpers ──────────────────────────────────────────────────────────────
const $ = id => document.getElementById(id);
const fmt = (n, d=1) => (+n || 0).toFixed(d);
const fmtVND = n => Math.max(0, +n || 0).toLocaleString('vi-VN');
function todayVN() {
  return new Date().toLocaleDateString('en-CA', { timeZone: 'Asia/Ho_Chi_Minh' });
}

// ── Status badge ─────────────────────────────────────────────────────────────
function setStatus(state, text) {
  const dot  = $('status-dot');
  const span = $('status-text');
  dot.className = state;   // 'ok' | 'warn' | 'err'
  span.textContent = text;
}

setStatus('warn', 'Đang kết nối…');

onValue(ref(db, '.info/connected'), snap => {
  setStatus(snap.val() ? 'ok' : 'warn', snap.val() ? 'Đã kết nối' : 'Đang kết nối…');
}, err => setStatus('err', 'Lỗi: ' + err.message));

// ── Chart defaults ───────────────────────────────────────────────────────────
Chart.defaults.color = '#7b82a0';
Chart.defaults.borderColor = '#2e3347';
Chart.defaults.font.family = 'Inter, sans-serif';
Chart.defaults.font.size = 11;

function makeChart(canvasId, type, datasets, options = {}) {
  const ctx = $(canvasId).getContext('2d');
  return new Chart(ctx, {
    type,
    data: { labels: [], datasets },
    options: {
      responsive: true,
      maintainAspectRatio: true,
      animation: { duration: 300 },
      plugins: { legend: { display: false }, tooltip: { mode: 'index', intersect: false } },
      scales: {
        x: { grid: { display: false }, ticks: { maxRotation: 0, maxTicksLimit: 8 } },
        y: { grid: { color: '#2e3347' }, ticks: { maxTicksLimit: 6 } }
      },
      ...options
    }
  });
}

// ── Realtime rolling chart (power, W) ────────────────────────────────────────
const MAX_POINTS = 60; // giữ tối đa 60 điểm gần nhất

const rtChart = makeChart('chart-realtime', 'line', [{
  label: 'Công suất (W)',
  data: [],
  borderColor: '#4f8ef7',
  backgroundColor: 'rgba(79,142,247,.12)',
  borderWidth: 2,
  fill: true,
  tension: 0.4,
  pointRadius: 0,
}]);

onValue(ref(db, 'realtime_chart'), snap => {
  if (!snap.exists()) return;
  const raw = snap.val(); // { key: {time, power} } hoặc array

  const points = Object.values(raw)
    .sort((a, b) => a.time.localeCompare(b.time))
    .slice(-MAX_POINTS);

  rtChart.data.labels = points.map(p => p.time.substring(11, 16)); // HH:MM
  rtChart.data.datasets[0].data = points.map(p => +(p.power || 0));
  rtChart.update('none'); // không animate để cảm giác "chạy liên tục"

  const last = points[points.length - 1];
  if (last) $('rt-sub').textContent = `Điểm gần nhất: ${last.time.substring(11,19)} · W`;
});

// ── Realtime stats ────────────────────────────────────────────────────────────
onValue(ref(db, 'realtime'), snap => {
  if (!snap.exists()) return;
  const d = snap.val();
  const m = d.metrics    || {};
  const c = d.consumption || {};

  $('s-voltage').textContent     = fmt(m.voltage, 1);
  $('s-current').textContent     = fmt(m.current, 2);
  $('s-power').textContent       = fmt((m.power || 0) / 1000, 2);
  $('s-freq').textContent        = fmt(m.frequency, 1);
  $('s-pf').textContent          = fmt(m.pf, 2);
  $('s-daily-kwh').textContent   = fmt(Math.max(0, c.daily_kwh   || 0), 2);
  $('s-monthly-kwh').textContent = fmt(Math.max(0, c.monthly_kwh || 0), 1);
  $('s-daily-cost').textContent  = fmtVND(c.daily_cost);
  $('s-monthly-cost').textContent= fmtVND(c.monthly_cost);

  if (d.timestamp) {
    const t = new Date(d.timestamp).toLocaleString('vi-VN', {
      timeZone: 'Asia/Ho_Chi_Minh',
      day: '2-digit', month: '2-digit', year: 'numeric',
      hour: '2-digit', minute: '2-digit', second: '2-digit',
    });
    $('update-time').textContent = t;
    $('last-updated').hidden = false;
  }
});

// ── Hourly history chart ──────────────────────────────────────────────────────
const histChart = makeChart('chart-history', 'line', [{
  label: 'kW',
  data: [],
  borderColor: '#7c6fff',
  backgroundColor: 'rgba(124,111,255,.1)',
  borderWidth: 2,
  fill: true,
  tension: 0.4,
  pointRadius: 3,
}]);

onValue(ref(db, 'history'), snap => {
  if (!snap.exists()) { histChart.data.labels = []; histChart.update(); return; }
  const histMap = snap.val();
  const today   = todayVN();
  const days    = Object.keys(histMap).sort();
  const dayKey  = histMap[today] && Object.keys(histMap[today]).length > 0
    ? today : days[days.length - 1];

  $('hist-sub').textContent = dayKey
    ? `Ngày ${dayKey}${dayKey !== today ? ' (dữ liệu gần nhất)' : ''} · đơn vị kW`
    : 'Đơn vị kW · múi giờ Việt Nam';

  const dayData = dayKey ? histMap[dayKey] : {};
  const sorted  = Object.keys(dayData).sort();
  histChart.data.labels              = sorted;
  histChart.data.datasets[0].data   = sorted.map(h => +((dayData[h]?.power || 0) / 1000).toFixed(2));
  histChart.update();
});

// ── 7-day bar chart ───────────────────────────────────────────────────────────
const dailyChart = makeChart('chart-daily', 'bar', [{
  label: 'kWh',
  data: [],
  backgroundColor: [],
  borderRadius: 6,
  maxBarThickness: 48,
}]);

onValue(ref(db, 'daily_usage'), snap => {
  if (!snap.exists()) { dailyChart.data.labels = []; dailyChart.update(); return; }
  const usageMap = snap.val();
  const days = Object.keys(usageMap).sort().slice(-7);
  const vals = days.map(d => +Number(usageMap[d]).toFixed(2));
  const colors = days.map((_, i) => i === days.length - 1 ? '#4f8ef7' : 'rgba(79,142,247,.3)');

  dailyChart.data.labels                   = days.map(d => d.substring(5));
  dailyChart.data.datasets[0].data         = vals;
  dailyChart.data.datasets[0].backgroundColor = colors;
  dailyChart.update();
});
