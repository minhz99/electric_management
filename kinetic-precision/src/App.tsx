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
import { ref, onValue } from 'firebase/database';
import { db, firebaseConfigured, getDatabaseHostLabel } from './firebase';

function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}

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
    <div
      className={cn(
        'rounded-xl border px-4 py-3.5',
        emphasis
          ? 'border-primary/25 bg-primary text-on-primary'
          : 'border-[var(--color-border)] bg-surface-elevated'
      )}
    >
      <div className="flex items-start justify-between gap-2">
        <p
          className={cn(
            'text-[11px] font-semibold uppercase tracking-wide',
            emphasis ? 'text-on-primary/85' : 'text-on-surface-muted'
          )}
        >
          {label}
        </p>
        {icon && <span className={emphasis ? 'text-on-primary/70' : 'text-on-surface-muted'}>{icon}</span>}
      </div>
      <p className={cn('mt-1.5 font-headline text-xl font-bold tabular-nums', emphasis ? '' : 'text-on-surface')}>
        {value}
        {unit ? <span className="ml-1 text-sm font-semibold opacity-90">{unit}</span> : null}
      </p>
    </div>
  );
}

function ChartPanel({ title, subtitle, children }: { title: string; subtitle?: string; children: ReactNode }) {
  return (
    <section className="panel p-5 md:p-6">
      <div className="mb-4">
        <h2 className="font-headline text-lg font-bold text-on-surface">{title}</h2>
        {subtitle ? <p className="mt-0.5 text-sm text-on-surface-muted">{subtitle}</p> : null}
      </div>
      {children}
    </section>
  );
}

export default function App() {
  const [realtimeData, setRealtimeData] = useState({
    metrics: EMPTY_METRICS,
    consumption: EMPTY_CONSUMPTION,
    timestamp: null as string | null,
  });
  const [powerHistory, setPowerHistory] = useState<{ time: string; power: number }[]>([]);
  const [historyChartDay, setHistoryChartDay] = useState<string | null>(null);
  const [dailyUsage, setDailyUsage] = useState<{ day: string; value: number }[]>([]);

  const [socketConnected, setSocketConnected] = useState(false);
  const [readError, setReadError] = useState<string | null>(null);
  const [connectTimeout, setConnectTimeout] = useState(false);

  useEffect(() => {
    if (!firebaseConfigured) return;

    const onDenied = (err: Error) => setReadError(err.message);

    const unsubSocket = onValue(
      ref(db, '.info/connected'),
      (snap) => setSocketConnected(snap.val() === true),
      onDenied
    );

    const unsubRt = onValue(
      ref(db, 'realtime'),
      (snapshot) => {
        if (snapshot.exists()) setRealtimeData(normalizeRealtime(snapshot.val()));
      },
      onDenied
    );

    const unsubHistory = onValue(
      ref(db, 'history'),
      (snapshot) => {
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
          setPowerHistory(
            Object.keys(dayHistory)
              .sort()
              .map((time) => {
                const p = dayHistory[time]?.power ?? 0;
                return { time, power: +(p / 1000).toFixed(2) };
              })
          );
        } else {
          setPowerHistory([]);
          setHistoryChartDay(null);
        }
      },
      onDenied
    );

    const unsubUsage = onValue(
      ref(db, 'daily_usage'),
      (snapshot) => {
        if (snapshot.exists()) {
          const usageMap = snapshot.val() as Record<string, number>;
          setDailyUsage(
            Object.keys(usageMap)
              .sort()
              .slice(-7)
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
      unsubHistory();
      unsubUsage();
    };
  }, []);

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

        <section className="grid grid-cols-2 gap-3 lg:grid-cols-4">
          <Stat label="Điện áp" value={realtimeData.metrics.voltage.toFixed(1)} unit="V" />
          <Stat label="Dòng điện" value={realtimeData.metrics.current.toFixed(2)} unit="A" />
          <Stat label="Công suất" value={(realtimeData.metrics.power / 1000).toFixed(2)} unit="kW" />
          <Stat label="Hệ số cos φ" value={realtimeData.metrics.pf.toFixed(2)} />
          <Stat label="Tần số" value={realtimeData.metrics.frequency.toFixed(1)} unit="Hz" />
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
          title="Công suất theo giờ"
          subtitle={
            historyChartDay
              ? `Ngày ${historyChartDay}${historyChartDay !== todayKeyVietnam() ? ' (dữ liệu gần nhất)' : ''} · đơn vị kW`
              : 'Đơn vị kW · múi giờ Việt Nam'
          }
        >
          <div className="h-[280px] w-full">
            <ResponsiveContainer width="100%" height="100%">
              {powerHistory.length > 0 ? (
                <AreaChart data={powerHistory}>
                  <defs>
                    <linearGradient id="gp" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="0%" stopColor="#0f6cbd" stopOpacity={0.2} />
                      <stop offset="100%" stopColor="#0f6cbd" stopOpacity={0} />
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
                  <Area type="monotone" dataKey="power" stroke="#0f6cbd" strokeWidth={2} fill="url(#gp)" />
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
                      <Cell key={i} fill={i === dailyUsage.length - 1 ? '#0f6cbd' : '#0f6cbd44'} />
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
