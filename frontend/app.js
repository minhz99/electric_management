/**
 * app.js — Electric Management Dashboard (Grafana-style)
 */

import { initializeApp } from 'https://www.gstatic.com/firebasejs/11.6.0/firebase-app.js';
import { getDatabase, ref, onValue } from 'https://www.gstatic.com/firebasejs/11.6.0/firebase-database.js';
import { FIREBASE_CONFIG } from './config.js';

// ── Init ─────────────────────────────────────────────────────────────────────
const app = initializeApp(FIREBASE_CONFIG);
const db  = getDatabase(app);
const $ = id => document.getElementById(id);

console.log("⚡ Dashboard: Đang khởi tạo kết nối Firebase...");

// ── State ────────────────────────────────────────────────────────────────────
let rtChart;
const MAX_RT_POINTS = 50; 

// ── Helpers ──────────────────────────────────────────────────────────────────
const fmt = (n, d=1) => (+n || 0).toFixed(d);
const fmtVND = n => Math.max(0, +n || 0).toLocaleString('vi-VN');

// ── Status ───────────────────────────────────────────────────────────────────
onValue(ref(db, '.info/connected'), snap => {
  const isOk = snap.val();
  $('status-dot').className = isOk ? 'ok' : 'warn';
  $('status-text').textContent = isOk ? 'Đã kết nối Firebase' : 'Mất kết nối...';
  if(isOk) console.log("✅ Firebase: Connected");
});

// ── Chart Initialization ─────────────────────────────────────────────────────
function initCharts() {
  Chart.defaults.color = 'rgba(255,255,255,0.4)';
  Chart.defaults.borderColor = 'rgba(255,255,255,0.05)';
  
  // 1. Realtime Rolling Chart (Grafana style)
  const ctxRt = $('chart-realtime').getContext('2d');
  const gradient = ctxRt.createLinearGradient(0, 0, 0, 200);
  gradient.addColorStop(0, 'rgba(79, 142, 247, 0.3)');
  gradient.addColorStop(1, 'rgba(79, 142, 247, 0)');

  rtChart = new Chart(ctxRt, {
    type: 'line',
    data: { labels: [], datasets: [{
      label: 'Công suất (W)',
      data: [],
      borderColor: '#4f8ef7',
      backgroundColor: gradient,
      fill: true,
      borderWidth: 2,
      tension: 0.4, // Tạo đường cong mượt
      pointRadius: 0,
      borderCapStyle: 'round'
    }]},
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: { duration: 800, easing: 'linear' }, // Animation mượt như Grafana
      plugins: { legend: { display: false } },
      scales: {
        x: { grid: { display: false }, ticks: { maxTicksLimit: 6 } },
        y: { 
            beginAtZero: true, 
            grid: { color: 'rgba(255,255,255,0.05)' },
            ticks: { callback: v => v + ' W' }
        }
      }
    }
  });
}

// ── Listen to Realtime Data ──────────────────────────────────────────────────
onValue(ref(db, 'realtime'), snap => {
  if (!snap.exists()) return;
  const { metrics: m, consumption: c, timestamp: ts } = snap.val();

  // Cập nhật text stats
  $('s-voltage').textContent     = fmt(m.voltage, 1);
  $('s-current').textContent     = fmt(m.current, 2);
  $('s-power').textContent       = fmt((m.power || 0) / 1000, 2);
  $('s-freq').textContent        = fmt(m.frequency, 1);
  $('s-pf').textContent          = fmt(m.pf, 2);
  $('s-daily-kwh').textContent   = fmt(c.daily_kwh, 2);
  $('s-monthly-kwh').textContent = fmt(c.monthly_kwh, 1);
  $('s-daily-cost').textContent  = fmtVND(c.daily_cost);
  $('s-monthly-cost').textContent= fmtVND(c.monthly_cost);

  if (ts) {
    $('update-time').textContent = new Date(ts).toLocaleTimeString('vi-VN');
    $('last-updated').hidden = false;
  }
});

// ── Listen to Realtime Chart Path ────────────────────────────────────────────
onValue(ref(db, 'realtime_chart'), snap => {
  if (!snap.exists()){
    console.log("ℹ️ RT Chart: Chưa có dữ liệu trên path /realtime_chart");
    return;
  }
  
  const raw = snap.val();
  const sortedPoints = Object.values(raw)
    .sort((a, b) => a.time.localeCompare(b.time))
    .slice(-MAX_RT_POINTS);

  rtChart.data.labels = sortedPoints.map(p => new Date(p.time).toLocaleTimeString('vi-VN', {hour:'2-digit', minute:'2-digit', second:'2-digit'}));
  rtChart.data.datasets[0].data = sortedPoints.map(p => p.power);
  
  // Update mượt mà
  rtChart.update(); 
  
  const last = sortedPoints[sortedPoints.length - 1];
  $('rt-sub').textContent = `Cập nhật cuối: ${new Date(last.time).toLocaleTimeString()} - ${last.power} W`;
});

// ── History & Daily Charts ───────────────────────────────────────────────────
// (Giữ nguyên logic cũ nhưng bọc trong các listener ổn định hơn)
onValue(ref(db, 'daily_usage'), snap => {
    // ... code vẽ bar chart tương tự như cũ ...
});

// Khởi chạy
document.addEventListener('DOMContentLoaded', () => {
    initCharts();
    console.log("🚀 Dashboard Ready!");
});
