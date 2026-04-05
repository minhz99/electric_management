/**
 * @license
 * SPDX-License-Identifier: Apache-2.0
 */

import { useState, useEffect, type ReactNode } from 'react';
import {
  Bell,
  Home,
  History,
  Cpu,
  Settings,
  ArrowUp,
  CheckCircle2,
  Wallet,
  Wifi,
  WifiOff,
  Loader2,
  Zap,
  AlertCircle,
} from 'lucide-react';
import { 
  LineChart, 
  Line, 
  XAxis, 
  YAxis, 
  CartesianGrid, 
  Tooltip, 
  ResponsiveContainer, 
  AreaChart, 
  Area,
  BarChart,
  Bar,
  Cell
} from 'recharts';
import { motion, AnimatePresence } from 'motion/react';
import { clsx, type ClassValue } from 'clsx';
import { twMerge } from 'tailwind-merge';
import { ref, onValue } from 'firebase/database';
import { db, firebaseConfigured } from './firebase';

function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}

/** Khớp processor.py: ngày theo GMT+7 (`datetime.now(TIMEZONE_GMT7)`). */
function todayKeyVietnam(): string {
  return new Date().toLocaleDateString('en-CA', { timeZone: 'Asia/Ho_Chi_Minh' });
}

const EMPTY_METRICS = { voltage: 0, current: 0, power: 0, frequency: 0, pf: 0 };
const EMPTY_CONSUMPTION = { daily_kwh: 0, monthly_kwh: 0, daily_cost: 0, monthly_cost: 0 };

function normalizeRealtime(raw: unknown): {
  metrics: typeof EMPTY_METRICS;
  consumption: typeof EMPTY_CONSUMPTION;
  timestamp: string | null;
} {
  const o = raw && typeof raw === 'object' ? (raw as Record<string, unknown>) : {};
  const m = o.metrics && typeof o.metrics === 'object' ? (o.metrics as Record<string, number>) : {};
  const c =
    o.consumption && typeof o.consumption === 'object' ? (o.consumption as Record<string, number>) : {};
  return {
    metrics: { ...EMPTY_METRICS, ...m },
    consumption: { ...EMPTY_CONSUMPTION, ...c },
    timestamp: typeof o.timestamp === 'string' ? o.timestamp : null,
  };
}

// --- Components ---

interface MetricCardProps {
  label: string;
  value: string | number;
  unit?: string;
  trend?: {
    value: string;
    type: 'up' | 'down' | 'neutral' | 'success';
  };
  highlighted?: boolean;
  icon?: ReactNode;
}

function MetricCard({ label, value, unit, trend, highlighted, icon }: MetricCardProps) {
  return (
    <motion.div 
      initial={{ opacity: 0, y: 20 }}
      animate={{ opacity: 1, y: 0 }}
      className={cn(
        "p-5 rounded-xl transition-all duration-300",
        highlighted 
          ? "bg-primary text-on-primary shadow-lg shadow-primary/20" 
          : "bg-surface-container-lowest border border-outline-variant/30 shadow-sm"
      )}
    >
      <div className="flex justify-between items-start mb-2">
        <p className={cn(
          "text-[10px] font-bold uppercase tracking-wider",
          highlighted ? "opacity-80" : "text-on-surface-variant"
        )}>
          {label}
        </p>
        {icon && <div className={cn(highlighted ? "opacity-50" : "text-on-surface-variant")}>{icon}</div>}
      </div>
      
      <div className="flex items-baseline justify-between">
        <h2 className={cn(
          "text-2xl font-extrabold font-headline",
          highlighted ? "text-on-primary" : "text-on-surface"
        )}>
          {value}
          {unit && <span className="text-xs font-medium ml-1">{unit}</span>}
        </h2>
        
        {trend && (
          <span className={cn(
            "text-[10px] flex items-center font-bold px-1.5 py-0.5 rounded-full",
            trend.type === 'up' && "text-error bg-error/10",
            trend.type === 'down' && "text-tertiary bg-tertiary/10",
            trend.type === 'success' && "text-tertiary bg-tertiary/10",
            trend.type === 'neutral' && "text-on-surface-variant bg-surface-container"
          )}>
            {trend.type === 'up' && <ArrowUp className="w-3 h-3 mr-0.5" />}
            {trend.type === 'success' && <CheckCircle2 className="w-3 h-3 mr-0.5" />}
            {trend.value}
          </span>
        )}
      </div>
    </motion.div>
  );
}

function FilterPill({ options, active, onChange }: { options: string[], active: string, onChange: (val: string) => void }) {
  return (
    <div className="glass-pill p-1 rounded-full flex gap-1 shadow-sm border border-white/40">
      {options.map((opt) => (
        <button
          key={opt}
          onClick={() => onChange(opt)}
          className={cn(
            "px-6 py-1.5 rounded-full text-sm transition-all duration-300",
            active === opt 
              ? "bg-surface-container-lowest text-primary font-bold shadow-sm" 
              : "text-on-surface-variant font-medium hover:bg-white/50"
          )}
        >
          {opt}
        </button>
      ))}
    </div>
  );
}

type ConnState = 'loading' | 'live' | 'error' | 'misconfigured';

export default function App() {
  const [powerFilter, setPowerFilter] = useState('Ngày');
  const [usageFilter, setUsageFilter] = useState('Ngày');

  const [realtimeData, setRealtimeData] = useState({
    metrics: EMPTY_METRICS,
    consumption: EMPTY_CONSUMPTION,
    timestamp: null as string | null,
  });

  const [powerHistory, setPowerHistory] = useState<{ time: string; power: number }[]>([]);
  const [historyChartDay, setHistoryChartDay] = useState<string | null>(null);
  const [dailyUsage, setDailyUsage] = useState<{ day: string; value: number }[]>([]);
  const [connState, setConnState] = useState<ConnState>(firebaseConfigured ? 'loading' : 'misconfigured');
  const [connError, setConnError] = useState<string | null>(null);

  useEffect(() => {
    if (!firebaseConfigured) {
      setConnState('misconfigured');
      return;
    }

    let sawAny = false;

    const markLive = () => {
      if (sawAny) setConnState('live');
    };

    const onDenied = (err: Error) => {
      setConnError(err.message);
      setConnState('error');
    };

    const rtRef = ref(db, 'realtime');
    const unsubRt = onValue(
      rtRef,
      (snapshot) => {
        sawAny = true;
        if (snapshot.exists()) {
          setRealtimeData(normalizeRealtime(snapshot.val()));
        }
        markLive();
      },
      onDenied
    );

    const historyRef = ref(db, 'history');
    const unsubHistory = onValue(
      historyRef,
      (snapshot) => {
        sawAny = true;
        if (snapshot.exists()) {
          const historyMap = snapshot.val() as Record<string, Record<string, { power?: number }>>;
          const todayStr = todayKeyVietnam();
          const sortedDays = Object.keys(historyMap).sort();
          const dayKey =
            historyMap[todayStr] && Object.keys(historyMap[todayStr]).length > 0
              ? todayStr
              : sortedDays[sortedDays.length - 1];
          setHistoryChartDay(dayKey ?? null);
          const dayHistory = dayKey ? historyMap[dayKey] || {} : {};
          const formatted = Object.keys(dayHistory)
            .sort()
            .map((time) => {
              const p = dayHistory[time]?.power ?? 0;
              return { time, power: +(p / 1000).toFixed(2) };
            });
          setPowerHistory(formatted);
        } else {
          setPowerHistory([]);
          setHistoryChartDay(null);
        }
        markLive();
      },
      onDenied
    );

    const usageRef = ref(db, 'daily_usage');
    const unsubUsage = onValue(
      usageRef,
      (snapshot) => {
        sawAny = true;
        if (snapshot.exists()) {
          const usageMap = snapshot.val() as Record<string, number>;
          const formatted = Object.keys(usageMap)
            .sort()
            .slice(-7)
            .map((date) => ({
              day: date.substring(5),
              value: +Number(usageMap[date]).toFixed(2),
            }));
          setDailyUsage(formatted);
        } else {
          setDailyUsage([]);
        }
        markLive();
      },
      onDenied
    );

    const t = window.setTimeout(() => {
      if (!sawAny) setConnState((s) => (s === 'loading' ? 'error' : s));
    }, 12000);

    return () => {
      window.clearTimeout(t);
      unsubRt();
      unsubHistory();
      unsubUsage();
    };
  }, []);

  const lastUpdatedLabel = realtimeData.timestamp
    ? new Date(realtimeData.timestamp).toLocaleString('vi-VN', {
        timeZone: 'Asia/Ho_Chi_Minh',
        day: '2-digit',
        month: '2-digit',
        year: 'numeric',
        hour: '2-digit',
        minute: '2-digit',
      })
    : null;

  return (
    <div className="min-h-screen kinetic-grid pb-20 md:pb-0">
      <header className="app-header-blur w-full sticky top-0 z-40 border-b border-outline-variant/15 shadow-sm shadow-primary/5">
        <div className="max-w-7xl mx-auto flex flex-wrap justify-between items-center gap-3 px-5 md:px-8 py-3.5">
          <div className="flex items-center gap-6 md:gap-10 min-w-0">
            <div className="flex items-center gap-2.5 min-w-0">
              <span className="flex h-9 w-9 shrink-0 items-center justify-center rounded-xl bg-primary text-on-primary shadow-md shadow-primary/25">
                <Zap className="w-5 h-5" aria-hidden />
              </span>
              <div className="min-w-0">
                <span className="block text-lg font-extrabold text-on-surface font-headline tracking-tight truncate">
                  Kinetic Precision
                </span>
                <span className="text-[11px] text-on-surface-variant font-medium hidden sm:block">
                  Giám sát điện năng thời gian thực
                </span>
              </div>
            </div>
            <nav className="hidden md:flex items-center gap-1">
              <a
                className="flex items-center gap-2 rounded-lg px-3 py-2 text-primary font-bold text-sm bg-primary/8"
                href="#"
              >
                <Home className="w-4 h-4 fill-current shrink-0" />
                <span>Trang chủ</span>
              </a>
              <a
                className="flex items-center gap-2 rounded-lg px-3 py-2 text-on-surface-variant hover:text-primary hover:bg-white/60 transition-colors text-sm font-medium"
                href="#"
              >
                <History className="w-4 h-4 shrink-0" />
                <span>Lịch sử</span>
              </a>
              <a
                className="flex items-center gap-2 rounded-lg px-3 py-2 text-on-surface-variant hover:text-primary hover:bg-white/60 transition-colors text-sm font-medium"
                href="#"
              >
                <Cpu className="w-4 h-4 shrink-0" />
                <span>Thiết bị</span>
              </a>
            </nav>
          </div>

          <div className="flex items-center gap-3 md:gap-5">
            <div
              className={cn(
                'hidden sm:flex items-center gap-2 rounded-full px-3 py-1.5 text-xs font-semibold border',
                connState === 'live' && 'bg-tertiary/10 text-tertiary border-tertiary/20',
                connState === 'loading' && 'bg-surface-container text-on-surface-variant border-outline-variant/30',
                connState === 'error' && 'bg-error/10 text-error border-error/20',
                connState === 'misconfigured' && 'bg-error/10 text-error border-error/20'
              )}
              title={connError || undefined}
            >
              {connState === 'loading' && <Loader2 className="w-3.5 h-3.5 animate-spin shrink-0" />}
              {connState === 'live' && <Wifi className="w-3.5 h-3.5 shrink-0" />}
              {(connState === 'error' || connState === 'misconfigured') && (
                <WifiOff className="w-3.5 h-3.5 shrink-0" />
              )}
              <span className="whitespace-nowrap">
                {connState === 'loading' && 'Đang kết nối…'}
                {connState === 'live' && 'Đã kết nối Firebase'}
                {connState === 'error' && 'Lỗi kết nối'}
                {connState === 'misconfigured' && 'Chưa cấu hình .env'}
              </span>
            </div>
            <button type="button" className="text-on-surface-variant hover:text-primary transition-colors p-1 rounded-lg hover:bg-white/50">
              <Bell className="w-5 h-5" />
            </button>
            <div className="h-8 w-px bg-outline-variant/25 hidden sm:block" />
            <div className="flex items-center gap-2.5 cursor-default group">
              <img
                alt=""
                className="w-8 h-8 rounded-full object-cover ring-2 ring-outline-variant/20"
                src="https://picsum.photos/seed/energy-dash/100/100"
                referrerPolicy="no-referrer"
              />
              <span className="hidden lg:inline text-on-surface font-bold font-headline text-sm truncate max-w-[8rem]">
                Hệ thống
              </span>
            </div>
          </div>
        </div>
      </header>

      {connState === 'misconfigured' && (
        <div className="max-w-7xl mx-auto px-5 md:px-8 pt-4">
          <div
            role="status"
            className="flex gap-3 rounded-2xl border border-error/25 bg-error/5 px-4 py-3 text-sm text-on-surface"
          >
            <AlertCircle className="w-5 h-5 text-error shrink-0 mt-0.5" />
            <div>
              <p className="font-bold text-error">Thiếu cấu hình Firebase cho web</p>
              <p className="text-on-surface-variant mt-1 leading-relaxed">
                Tạo file <code className="text-on-surface font-mono text-xs bg-surface-container px-1 py-0.5 rounded">kinetic-precision/.env</code> từ{' '}
                <code className="text-on-surface font-mono text-xs bg-surface-container px-1 py-0.5 rounded">.env.example</code> và điền{' '}
                <code className="text-on-surface font-mono text-xs bg-surface-container px-1 py-0.5 rounded">VITE_FIREBASE_DATABASE_URL</code> trùng với
                Realtime Database đang có dữ liệu (ví dụ URL dạng{' '}
                <code className="text-on-surface font-mono text-xs bg-surface-container px-1 py-0.5 rounded">…-default-rtdb…</code>), rồi chạy lại{' '}
                <code className="text-on-surface font-mono text-xs bg-surface-container px-1 py-0.5 rounded">npm run dev</code>.
              </p>
            </div>
          </div>
        </div>
      )}

      {connState === 'error' && connError && (
        <div className="max-w-7xl mx-auto px-5 md:px-8 pt-4">
          <div className="rounded-2xl border border-error/25 bg-error/5 px-4 py-3 text-sm text-error">
            <span className="font-semibold">Firebase: </span>
            {connError}
          </div>
        </div>
      )}

      <main className="max-w-7xl mx-auto p-5 md:p-8 space-y-8">
        {lastUpdatedLabel && connState === 'live' && (
          <p className="text-xs text-on-surface-variant font-medium -mt-2 md:-mt-1">
            Cập nhật gần nhất (VN): <span className="text-on-surface">{lastUpdatedLabel}</span>
          </p>
        )}
        {/* Metric Cards Grid */}
        <section className="grid grid-cols-1 sm:grid-cols-2 md:grid-cols-3 lg:grid-cols-5 gap-4">
          <MetricCard
            label="Điện áp (V)"
            value={realtimeData.metrics.voltage.toFixed(1)}
            trend={realtimeData.metrics.voltage > 0 ? { value: 'Tốt', type: 'success' } : undefined} 
          />
          <MetricCard 
            label="Cường Độ (A)" 
            value={realtimeData.metrics.current.toFixed(2)} 
          />
          <MetricCard 
            label="Hệ Số PF" 
            value={realtimeData.metrics.pf.toFixed(2)} 
          />
          <MetricCard 
            label="Tần Số (Hz)" 
            value={realtimeData.metrics.frequency.toFixed(1)} 
          />
          <MetricCard 
            label="Công suất hiện tại" 
            value={(realtimeData.metrics.power / 1000).toFixed(2)} 
            unit="kW" 
          />
          
          <MetricCard 
            label="Điện năng hôm nay" 
            value={Math.max(0, realtimeData.consumption.daily_kwh).toFixed(2)} 
            unit="kWh" 
          />
          <MetricCard 
            label="Điện năng tháng này" 
            value={Math.max(0, realtimeData.consumption.monthly_kwh).toFixed(1)} 
            unit="kWh" 
          />
          <MetricCard 
            label="Tiền điện hôm nay" 
            value={Math.max(0, realtimeData.consumption.daily_cost).toLocaleString('vi-VN')} 
            unit="đ" 
          />
          
          <div className="lg:col-span-2">
            <MetricCard 
              label="Tiền điện tạm tính (Tháng này)" 
              value={Math.max(0, realtimeData.consumption.monthly_cost).toLocaleString('vi-VN')} 
              unit="đ" 
              highlighted 
              icon={<Wallet className="w-5 h-5" />}
            />
          </div>
        </section>

        {/* Power Chart Section */}
        <motion.section
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.2 }}
          className="bg-surface-container-lowest/90 rounded-2xl p-6 md:p-8 border border-outline-variant/20 shadow-lg shadow-primary/5"
        >
          <div className="flex flex-col md:flex-row justify-between items-start md:items-center gap-4 mb-10">
            <div>
              <h3 className="text-2xl font-bold font-headline text-on-surface">Công suất điện theo thời gian</h3>
              <p className="text-sm text-on-surface-variant mt-1">
                Theo dõi tải theo giờ (kW), mốc thời gian theo Việt Nam
                {historyChartDay && (
                  <span className="text-on-surface font-medium">
                    {' '}
                    · Đang xem: {historyChartDay}
                    {historyChartDay !== todayKeyVietnam() ? ' (ngày có dữ liệu gần nhất)' : ''}
                  </span>
                )}
              </p>
            </div>
            <FilterPill 
              options={['Ngày', 'Tuần', 'Tháng']} 
              active={powerFilter} 
              onChange={setPowerFilter} 
            />
          </div>
          
          <div className="h-[350px] w-full">
            <ResponsiveContainer width="100%" height="100%">
              {powerHistory.length > 0 ? (
                <AreaChart data={powerHistory}>
                  <defs>
                    <linearGradient id="colorPower" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="5%" stopColor="#005FB8" stopOpacity={0.2}/>
                      <stop offset="95%" stopColor="#005FB8" stopOpacity={0}/>
                    </linearGradient>
                  </defs>
                  <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#e1e3e4" />
                  <XAxis 
                    dataKey="time" 
                    axisLine={false} 
                    tickLine={false} 
                    tick={{ fontSize: 10, fill: '#424752', fontWeight: 600 }}
                    dy={10}
                  />
                  <YAxis hide />
                  <Tooltip 
                    contentStyle={{ 
                      backgroundColor: '#191c1d', 
                      border: 'none', 
                      borderRadius: '8px',
                      color: '#ffffff',
                      fontSize: '12px'
                    }}
                    itemStyle={{ color: '#ffffff' }}
                  />
                  <Area 
                    type="monotone" 
                    dataKey="power" 
                    stroke="#005FB8" 
                    strokeWidth={3}
                    fillOpacity={1} 
                    fill="url(#colorPower)" 
                    animationDuration={1500}
                  />
                </AreaChart>
              ) : (
                <div className="flex flex-col items-center justify-center h-full gap-2 text-center px-6 text-on-surface-variant">
                  <span className="text-sm font-medium">Chưa có điểm lịch sử</span>
                  <span className="text-xs max-w-md">
                    Kiểm tra node <code className="font-mono text-[11px] bg-surface-container px-1 rounded">history</code> trên RTDB và quyền đọc (Rules).
                  </span>
                </div>
              )}
            </ResponsiveContainer>
          </div>
        </motion.section>

        {/* Usage Chart Section */}
        <motion.section
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.3 }}
          className="bg-surface-container-lowest/90 rounded-2xl p-6 md:p-8 border border-outline-variant/20 shadow-lg shadow-primary/5"
        >
          <div className="flex flex-col md:flex-row justify-between items-start md:items-center gap-4 mb-10">
            <div>
              <h3 className="text-2xl font-bold font-headline text-on-surface">Lượng điện sử dụng 7 ngày qua</h3>
              <p className="text-sm text-on-surface-variant">Phân tích điện năng tiêu thụ theo chu kỳ</p>
            </div>
            <div className="flex gap-2 bg-surface-container p-1 rounded-full">
              {['Ngày', 'Tháng', 'Năm'].map(opt => (
                <button
                  key={opt}
                  onClick={() => setUsageFilter(opt)}
                  className={cn(
                    "px-4 py-1.5 rounded-full text-xs transition-all duration-300",
                    usageFilter === opt 
                      ? "bg-surface-container-lowest text-primary font-bold shadow-sm" 
                      : "text-on-surface-variant font-medium hover:bg-white/50"
                  )}
                >
                  {opt}
                </button>
              ))}
            </div>
          </div>

          <div className="h-[300px] w-full">
            <ResponsiveContainer width="100%" height="100%">
              {dailyUsage.length > 0 ? (
                <BarChart data={dailyUsage} margin={{ top: 20, right: 0, left: 0, bottom: 0 }}>
                  <XAxis 
                    dataKey="day" 
                    axisLine={false} 
                    tickLine={false} 
                    tick={{ fontSize: 10, fill: '#424752', fontWeight: 600 }}
                    dy={10}
                  />
                  <Tooltip cursor={{ fill: 'transparent' }} contentStyle={{ borderRadius: '8px' }} />
                  <Bar dataKey="value" name="Điện tiêu thụ (kWh)" radius={[4, 4, 0, 0]} barSize={60}>
                    {dailyUsage.map((entry, index) => (
                      <Cell 
                        key={`cell-${index}`} 
                        fill={index === dailyUsage.length - 1 ? '#005FB8' : '#005FB833'} 
                        className="transition-all duration-300 hover:fill-primary/60"
                      />
                    ))}
                  </Bar>
                </BarChart>
              ) : (
                <div className="flex items-center justify-center h-full text-on-surface-variant">Chưa có dữ liệu 7 ngày qua</div>
              )}
            </ResponsiveContainer>
          </div>
        </motion.section>
      </main>

      {/* Bottom Nav (Mobile) */}
      <nav className="md:hidden fixed bottom-0 left-0 right-0 bg-white/90 backdrop-blur-lg flex justify-around items-center py-3 px-4 z-50 border-t border-outline-variant/10">
        <div className="flex flex-col items-center gap-1 text-primary">
          <Home className="w-5 h-5 fill-current" />
          <span className="text-[10px] font-bold">Trang Chủ</span>
        </div>
        <div className="flex flex-col items-center gap-1 text-on-surface-variant">
          <History className="w-5 h-5" />
          <span className="text-[10px] font-medium">Lịch Sử</span>
        </div>
        <div className="flex flex-col items-center gap-1 text-on-surface-variant">
          <Cpu className="w-5 h-5" />
          <span className="text-[10px] font-medium">Thiết Bị</span>
        </div>
        <div className="flex flex-col items-center gap-1 text-on-surface-variant">
          <Settings className="w-5 h-5" />
          <span className="text-[10px] font-medium">Cài Đặt</span>
        </div>
      </nav>
    </div>
  );
}
