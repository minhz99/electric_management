/**
 * @license
 * SPDX-License-Identifier: Apache-2.0
 */

import { useState, useEffect, type ReactNode } from 'react';
import { Activity, AlertCircle, CheckCircle2, Database, Loader2, Wallet, Zap } from 'lucide-react';
import {
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  AreaChart,
  Area,
  BarChart,
  Bar,
  Cell,
} from 'recharts';
import { clsx, type ClassValue } from 'clsx';
import { twMerge } from 'tailwind-merge';
import { ref, onValue, query, orderByKey, limitToLast } from 'firebase/database';
import { db, firebaseConfigured, getDatabaseHostLabel } from './firebase';

function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}

function todayKeyVietnam(): string {
  return new Date().toLocaleDateString('en-CA', { timeZone: 'Asia/Ho_Chi_Minh' });
}

const EMPTY_METRICS = { voltage: 0, current: 0, power: 0, frequency: 0, pf: 0 };
const EMPTY_CONSUMPTION = { daily_kwh: 0, monthly_kwh: 0, total_kwh: 0, daily_cost: 0, monthly_cost: 0 };
const MAX_LIVE_POINTS = 120;
type DayHistoryMap = Record<string, { power?: number }>;
type LiveChartPoint = { time: string; power: number };

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

function normalizeDayHistory(dayHistory: DayHistoryMap): { time: string; power: number }[] {
  return Object.keys(dayHistory)
    .sort()
    .map((time) => {
      const p = dayHistory[time]?.power ?? 0;
      return { time, power: +(p / 1000).toFixed(2) };
    });
}

function Stat({
  label,
  value,
  unit,
  emphasis,
  icon,
}: {
  label: string;
  value: string;
  unit?: string;
  emphasis?: boolean;
  icon?: ReactNode;
}) {
  return (
    <div className={cn("stat-card flex flex-col justify-between group", emphasis && "emphasis")}>
      <div className="flex items-center justify-between mb-4">
        <p
          className={cn(
            'text-xs font-semibold tracking-wider uppercase',
            emphasis ? 'text-white/80' : 'text-on-surface-muted group-hover:text-primary transition-colors duration-300'
          )}
        >
          {label}
        </p>
        {icon && (
          <div className={cn(
            "p-2 rounded-full",
            emphasis ? "bg-white/20 text-white" : "bg-primary/10 text-primary"
          )}>
            {icon}
          </div>
        )}
      </div>
      <div className="flex items-baseline gap-1.5">
        <span className={cn('text-3xl font-bold tracking-tight', emphasis ? '' : 'text-on-surface')}>
          {value}
        </span>
        {unit && <span className={cn("text-sm font-medium", emphasis ? "text-white/80" : "text-on-surface-muted")}>{unit}</span>}
      </div>
    </div>
  );
}

function ChartPanel({ title, subtitle, children }: { title: string; subtitle?: string; children: ReactNode }) {
  return (
    <section className="panel p-6 md:p-8">
      <div className="mb-6 flex flex-col sm:flex-row sm:items-center justify-between gap-2">
        <div>
          <h2 className="font-headline text-xl font-bold text-on-surface">{title}</h2>
          {subtitle && <p className="mt-1 text-[13px] text-on-surface-muted">{subtitle}</p>}
        </div>
      </div>
      {children}
    </section>
  );
}

export default function App() {
  const [todayKey, setTodayKey] = useState(todayKeyVietnam());
  const [realtimeData, setRealtimeData] = useState({
    metrics: EMPTY_METRICS,
    consumption: EMPTY_CONSUMPTION,
    timestamp: null as string | null,
  });
  const [livePower, setLivePower] = useState<LiveChartPoint[]>([]);
  const [powerHistory, setPowerHistory] = useState<{ time: string; power: number }[]>([]);
  const [historyChartDay, setHistoryChartDay] = useState<string | null>(null);
  const [dailyUsage, setDailyUsage] = useState<{ day: string; value: number }[]>([]);

  const [socketConnected, setSocketConnected] = useState(false);
  const [readError, setReadError] = useState<string | null>(null);
  const [connectTimeout, setConnectTimeout] = useState(false);

  useEffect(() => {
    const syncTodayKey = () => {
      const next = todayKeyVietnam();
      setTodayKey((prev) => (prev === next ? prev : next));
    };

    syncTodayKey();
    const id = window.setInterval(syncTodayKey, 60_000);
    return () => window.clearInterval(id);
  }, []);

  useEffect(() => {
    if (!firebaseConfigured) return;

    const onDenied = (err: Error) => setReadError(err.message);
    let todayHistory: DayHistoryMap | null = null;
    let fallbackHistory: { dayKey: string; values: DayHistoryMap } | null = null;

    const syncHistoryChart = () => {
      if (todayHistory && Object.keys(todayHistory).length > 0) {
        setHistoryChartDay(todayKey);
        setPowerHistory(normalizeDayHistory(todayHistory));
        return;
      }

      if (fallbackHistory) {
        setHistoryChartDay(fallbackHistory.dayKey);
        setPowerHistory(normalizeDayHistory(fallbackHistory.values));
        return;
      }

      setHistoryChartDay(null);
      setPowerHistory([]);
    };

    const unsubSocket = onValue(
      ref(db, '.info/connected'),
      (snap) => setSocketConnected(snap.val() === true),
      onDenied
    );

    const unsubRt = onValue(
      ref(db, 'realtime'),
      (snapshot) => {
        if (!snapshot.exists()) return;
        const nextRealtime = normalizeRealtime(snapshot.val());
        setRealtimeData(nextRealtime);

        if (!nextRealtime.timestamp) return;

        const timeLabel = new Date(nextRealtime.timestamp).toLocaleTimeString('vi-VN', {
          hour: '2-digit',
          minute: '2-digit',
          second: '2-digit',
          timeZone: 'Asia/Ho_Chi_Minh',
        });
        const point = {
          time: timeLabel,
          power: +nextRealtime.metrics.power.toFixed(1),
        };

        setLivePower((prev) => {
          const last = prev[prev.length - 1];
          if (last && last.time === point.time && last.power === point.power) return prev;
          return [...prev, point].slice(-MAX_LIVE_POINTS);
        });
      },
      onDenied
    );

    const unsubTodayHistory = onValue(
      ref(db, `history/${todayKey}`),
      (snapshot) => {
        todayHistory = snapshot.exists() ? (snapshot.val() as DayHistoryMap) : null;
        syncHistoryChart();
      },
      onDenied
    );

    const unsubLatestHistory = onValue(
      query(ref(db, 'history'), orderByKey(), limitToLast(1)),
      (snapshot) => {
        if (snapshot.exists()) {
          const latest = Object.entries(snapshot.val() as Record<string, DayHistoryMap>).at(0);
          fallbackHistory = latest ? { dayKey: latest[0], values: latest[1] } : null;
        } else {
          fallbackHistory = null;
        }
        syncHistoryChart();
      },
      onDenied
    );

    const unsubUsage = onValue(
      query(ref(db, 'daily_usage'), orderByKey(), limitToLast(7)),
      (snapshot) => {
        if (snapshot.exists()) {
          const usageMap = snapshot.val() as Record<string, number>;
          setDailyUsage(
            Object.keys(usageMap)
              .sort()
              .map((date) => ({
                day: date.substring(5),
                value: +Number(usageMap[date]).toFixed(2),
              }))
          );
        } else setDailyUsage([]);
      },
      onDenied
    );

    const t = window.setTimeout(() => {
      setConnectTimeout(true);
    }, 10000);

    return () => {
      window.clearTimeout(t);
      unsubSocket();
      unsubRt();
      unsubTodayHistory();
      unsubLatestHistory();
      unsubUsage();
    };
  }, [todayKey]);

  const lastUpdated =
    realtimeData.timestamp &&
    new Date(realtimeData.timestamp).toLocaleString('vi-VN', {
      timeZone: 'Asia/Ho_Chi_Minh',
      day: '2-digit',
      month: '2-digit',
      year: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
    });

  const host = getDatabaseHostLabel();

  const status = !firebaseConfigured
    ? { tone: 'danger' as const, label: 'Chưa cấu hình', detail: 'Thiếu VITE_FIREBASE_DATABASE_URL hợp lệ trong .env' }
    : readError
      ? { tone: 'danger' as const, label: 'Lỗi đọc dữ liệu', detail: readError }
      : connectTimeout && !socketConnected
        ? { tone: 'danger' as const, label: 'Không kết nối được', detail: 'Kiểm tra URL RTDB và mạng' }
        : !socketConnected
          ? { tone: 'warn' as const, label: 'Đang kết nối…', detail: host }
          : { tone: 'ok' as const, label: 'Realtime Database', detail: host };

  return (
    <div className="page-shell pb-10">
      <header className="sticky top-0 z-10 border-b border-[var(--color-border)] bg-surface-elevated/95 backdrop-blur-sm">
        <div className="mx-auto flex max-w-5xl flex-col gap-3 px-4 py-4 sm:flex-row sm:items-center sm:justify-between">
          <div className="flex items-center gap-3">
            <span className="flex h-10 w-10 shrink-0 items-center justify-center rounded-xl bg-primary text-on-primary">
              <Zap className="h-5 w-5" aria-hidden />
            </span>
            <div>
              <h1 className="font-headline text-lg font-bold tracking-tight text-on-surface">Điện năng</h1>
              <p className="text-xs text-on-surface-muted">Theo dõi theo dữ liệu Firebase RTDB</p>
            </div>
          </div>

          <div
            className={cn(
              'flex items-start gap-2 rounded-xl border px-3 py-2.5 text-sm',
              status.tone === 'ok' && 'border-transparent bg-ok-bg text-ok',
              status.tone === 'warn' && 'border-transparent bg-warn-bg text-warn',
              status.tone === 'danger' && 'border-transparent bg-danger-bg text-danger'
            )}
          >
            {status.tone === 'warn' ? (
              <Loader2 className="mt-0.5 h-4 w-4 shrink-0 animate-spin" aria-hidden />
            ) : status.tone === 'ok' ? (
              <CheckCircle2 className="mt-0.5 h-4 w-4 shrink-0" aria-hidden />
            ) : (
              <AlertCircle className="mt-0.5 h-4 w-4 shrink-0" aria-hidden />
            )}
            <div className="min-w-0">
              <p className="flex items-center gap-1.5 font-semibold">
                <Database className="h-3.5 w-3.5 shrink-0 opacity-80" aria-hidden />
                {status.label}
              </p>
              <p className="mt-0.5 break-all text-xs opacity-90">{status.detail}</p>
            </div>
          </div>
        </div>
      </header>

      <div className="mx-auto max-w-5xl space-y-6 px-4 pt-6">
        {!firebaseConfigured ? (
          <div className="panel flex gap-3 border-danger/20 bg-danger-bg p-4 text-sm text-danger">
            <AlertCircle className="h-5 w-5 shrink-0" />
            <div>
              <p className="font-semibold">Cấu hình tối thiểu cho web</p>
              <ol className="mt-2 list-decimal space-y-1 pl-4 text-on-surface">
                <li>
                  Tạo <code className="rounded bg-surface px-1 py-0.5 font-mono text-xs">kinetic-precision/.env</code>{' '}
                  từ <code className="rounded bg-surface px-1 py-0.5 font-mono text-xs">.env.example</code>.
                </li>
                <li>
                  Đặt <code className="rounded bg-surface px-1 py-0.5 font-mono text-xs">VITE_FIREBASE_DATABASE_URL</code>{' '}
                  = URL Realtime Database (HTTPS, domain <span className="font-mono">firebaseio.com</span> hoặc{' '}
                  <span className="font-mono">firebasedatabase.app</span>).
                </li>
                <li>
                  Điền thêm các biến <span className="font-mono">VITE_FIREBASE_*</span> từ Firebase Console → Project settings → Your apps.
                </li>
                <li>
                  Khởi động lại <code className="rounded bg-surface px-1 py-0.5 font-mono text-xs">npm run dev</code>.
                </li>
              </ol>
            </div>
          </div>
        ) : null}

        {firebaseConfigured && readError ? (
          <div className="panel border-danger/20 bg-danger-bg p-4 text-sm text-danger">
            <span className="font-semibold">Firebase từ chối truy cập: </span>
            {readError}
            <p className="mt-2 text-on-surface">
              Mở Firebase Console → Realtime Database → Rules và cho phép <code className="font-mono text-xs">read</code> phù hợp
              (ví dụ thử nghiệm: <code className="font-mono text-xs">.read: true</code>).
            </p>
          </div>
        ) : null}

        {lastUpdated && socketConnected && !readError ? (
          <p className="text-xs text-on-surface-muted">
            Cập nhật gần nhất (GMT+7): <span className="font-medium text-on-surface">{lastUpdated}</span>
          </p>
        ) : null}

        <section className="grid grid-cols-2 gap-3 lg:grid-cols-5">
          <Stat label="Điện áp" value={realtimeData.metrics.voltage.toFixed(1)} unit="V" />
          <Stat label="Dòng điện" value={realtimeData.metrics.current.toFixed(2)} unit="A" />
          <Stat label="Công suất" value={(realtimeData.metrics.power / 1000).toFixed(2)} unit="kW" />
          <Stat label="Hệ số cos φ" value={realtimeData.metrics.pf.toFixed(2)} />
          <Stat label="Tần số" value={realtimeData.metrics.frequency.toFixed(1)} unit="Hz" />
          <Stat label="Tổng điện năng" value={Math.max(0, realtimeData.consumption.total_kwh).toFixed(1)} unit="kWh" />
          <Stat label="Hôm nay" value={Math.max(0, realtimeData.consumption.daily_kwh).toFixed(2)} unit="kWh" />
          <Stat label="Tháng này" value={Math.max(0, realtimeData.consumption.monthly_kwh).toFixed(1)} unit="kWh" />
          <Stat
            label="Tiền hôm nay"
            value={Math.max(0, realtimeData.consumption.daily_cost).toLocaleString('vi-VN')}
            unit="đ"
          />
          <Stat
            label="Tiền tháng (tạm tính)"
            value={Math.max(0, realtimeData.consumption.monthly_cost).toLocaleString('vi-VN')}
            unit="đ"
            emphasis
            icon={<Wallet className="h-4 w-4" />}
          />
        </section>

        <ChartPanel
          title="Công suất thời gian thực"
          subtitle={
            livePower.length > 0
              ? `${livePower.length} mẫu gần nhất từ nhánh realtime · đơn vị W`
              : 'Đồ thị chạy trực tiếp từ nhánh realtime · đơn vị W'
          }
        >
          <div className="h-[280px] w-full">
            <ResponsiveContainer width="100%" height="100%">
              {livePower.length > 0 ? (
                <AreaChart data={livePower}>
                  <defs>
                    <linearGradient id="livePowerGradient" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="0%" stopColor="#0ea5e9" stopOpacity={0.28} />
                      <stop offset="100%" stopColor="#0ea5e9" stopOpacity={0} />
                    </linearGradient>
                  </defs>
                  <CartesianGrid strokeDasharray="4 4" vertical={false} stroke="var(--color-border)" />
                  <XAxis dataKey="time" axisLine={false} tickLine={false} tick={{ fontSize: 11, fill: '#5c6370' }} dy={8} minTickGap={24} />
                  <YAxis
                    width={52}
                    axisLine={false}
                    tickLine={false}
                    tick={{ fontSize: 11, fill: '#5c6370' }}
                    tickFormatter={(value) => `${value}W`}
                  />
                  <Tooltip
                    formatter={(value: number) => [`${value} W`, 'Công suất']}
                    contentStyle={{
                      borderRadius: '10px',
                      border: '1px solid var(--color-border)',
                      fontSize: '12px',
                    }}
                  />
                  <Area type="monotone" dataKey="power" stroke="#0ea5e9" strokeWidth={2} fill="url(#livePowerGradient)" />
                </AreaChart>
              ) : (
                <div className="flex h-full flex-col items-center justify-center gap-2 text-center text-sm text-on-surface-muted">
                  <Activity className="h-8 w-8 opacity-40" />
                  <p>Đang chờ mẫu realtime đầu tiên từ Firebase</p>
                </div>
              )}
            </ResponsiveContainer>
          </div>
        </ChartPanel>

        <ChartPanel
          title="Công suất theo giờ"
          subtitle={
            historyChartDay
              ? `Ngày ${historyChartDay}${historyChartDay !== todayKey ? ' (dữ liệu gần nhất)' : ''} · đơn vị kW`
              : 'Đơn vị kW · múi giờ Việt Nam'
          }
        >
          <div className="h-[280px] w-full">
            <ResponsiveContainer width="100%" height="100%">
              {powerHistory.length > 0 ? (
                <AreaChart data={powerHistory}>
                  <defs>
                    <linearGradient id="gp" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="0%" stopColor="#3b82f6" stopOpacity={0.2} />
                      <stop offset="100%" stopColor="#3b82f6" stopOpacity={0} />
                    </linearGradient>
                  </defs>
                  <CartesianGrid strokeDasharray="4 4" vertical={false} stroke="var(--color-border)" />
                  <XAxis dataKey="time" axisLine={false} tickLine={false} tick={{ fontSize: 11, fill: '#5c6370' }} dy={8} />
                  <YAxis width={36} axisLine={false} tickLine={false} tick={{ fontSize: 11, fill: '#5c6370' }} />
                  <Tooltip
                    contentStyle={{
                      borderRadius: '10px',
                      border: '1px solid var(--color-border)',
                      fontSize: '12px',
                    }}
                  />
                  <Area type="monotone" dataKey="power" stroke="#3b82f6" strokeWidth={2} fill="url(#gp)" />
                </AreaChart>
              ) : (
                <div className="flex h-full flex-col items-center justify-center gap-2 text-center text-sm text-on-surface-muted">
                  <Activity className="h-8 w-8 opacity-40" />
                  <p>Chưa có dữ liệu nhánh <span className="font-mono text-xs">history</span></p>
                </div>
              )}
            </ResponsiveContainer>
          </div>
        </ChartPanel>

        <ChartPanel title="Điện năng 7 ngày gần nhất" subtitle="kWh theo ngày (MM-DD)">
          <div className="h-[260px] w-full">
            <ResponsiveContainer width="100%" height="100%">
              {dailyUsage.length > 0 ? (
                <BarChart data={dailyUsage} margin={{ top: 8, right: 8, left: 0, bottom: 0 }}>
                  <XAxis dataKey="day" axisLine={false} tickLine={false} tick={{ fontSize: 11, fill: '#5c6370' }} dy={8} />
                  <Tooltip cursor={{ fill: 'transparent' }} contentStyle={{ borderRadius: '10px', fontSize: '12px' }} />
                  <Bar dataKey="value" name="kWh" radius={[6, 6, 0, 0]} maxBarSize={48}>
                    {dailyUsage.map((_, i) => (
                      <Cell key={i} fill={i === dailyUsage.length - 1 ? '#3b82f6' : '#3b82f644'} />
                    ))}
                  </Bar>
                </BarChart>
              ) : (
                <div className="flex h-full items-center justify-center text-sm text-on-surface-muted">
                  Chưa có dữ liệu nhánh <span className="ml-1 font-mono text-xs">daily_usage</span>
                </div>
              )}
            </ResponsiveContainer>
          </div>
        </ChartPanel>
      </div>
    </div>
  );
}
