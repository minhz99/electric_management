/**
 * @license
 * SPDX-License-Identifier: Apache-2.0
 */

import { useEffect, useState, type ReactNode } from 'react';
import { Activity, AlertCircle, CheckCircle2, Database, Loader2, Wallet, Zap } from 'lucide-react';
import { Area, AreaChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts';
import { clsx, type ClassValue } from 'clsx';
import { twMerge } from 'tailwind-merge';
import { limitToLast, onValue, orderByKey, query, ref } from 'firebase/database';
import { db, firebaseConfigured, getDatabaseHostLabel } from './firebase';

function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}

const EMPTY_METRICS = { voltage: 0, current: 0, power: 0, frequency: 0, pf: 0 };
const EMPTY_CONSUMPTION = { daily_kwh: 0, monthly_kwh: 0, total_kwh: 0, daily_cost: 0, monthly_cost: 0 };

const RANGE_OPTIONS = [
  { key: 'hour', label: 'Giờ' },
  { key: 'day', label: 'Ngày' },
  { key: 'month', label: 'Tháng' },
  { key: 'year', label: 'Năm' },
  { key: 'all', label: 'All' },
] as const;

type ChartRange = (typeof RANGE_OPTIONS)[number]['key'];
type RealtimeState = {
  metrics: typeof EMPTY_METRICS;
  consumption: typeof EMPTY_CONSUMPTION;
  timestamp: string | null;
};
type RecentPowerPoint = { iso: string; power: number };
type HourlyPoint = { iso: string; powerKw: number };
type DailyUsagePoint = { dateKey: string; value: number };
type TrendPoint = { label: string; value: number; tooltipLabel: string };
type TrendModel = {
  points: TrendPoint[];
  subtitle: string;
  emptyMessage: string;
  seriesLabel: string;
  stroke: string;
  fillId: string;
  yTickFormatter: (value: number) => string;
  tooltipValueFormatter: (value: number) => string;
};

function normalizeRealtime(raw: unknown): RealtimeState {
  const o = raw && typeof raw === 'object' ? (raw as Record<string, unknown>) : {};
  const m = o.metrics && typeof o.metrics === 'object' ? (o.metrics as Record<string, number>) : {};
  const c = o.consumption && typeof o.consumption === 'object' ? (o.consumption as Record<string, number>) : {};

  return {
    metrics: { ...EMPTY_METRICS, ...m },
    consumption: { ...EMPTY_CONSUMPTION, ...c },
    timestamp: typeof o.timestamp === 'string' ? o.timestamp : null,
  };
}

function formatDateTime(iso: string, options: Intl.DateTimeFormatOptions): string {
  return new Date(iso).toLocaleString('vi-VN', { timeZone: 'Asia/Ho_Chi_Minh', ...options });
}

function parseDailyUsage(raw: unknown): DailyUsagePoint[] {
  const usageMap = raw && typeof raw === 'object' ? (raw as Record<string, number>) : {};
  return Object.keys(usageMap)
    .sort()
    .map((dateKey) => ({
      dateKey,
      value: +Number(usageMap[dateKey]).toFixed(3),
    }));
}

function parseHourlyHistory(raw: unknown): HourlyPoint[] {
  const historyMap = raw && typeof raw === 'object' ? (raw as Record<string, Record<string, { power?: number }>>) : {};

  return Object.keys(historyMap)
    .sort()
    .flatMap((dateKey) =>
      Object.keys(historyMap[dateKey] || {})
        .sort()
        .map((hourKey) => ({
          iso: `${dateKey}T${hourKey}:00+07:00`,
          powerKw: +(((historyMap[dateKey]?.[hourKey]?.power ?? 0) as number) / 1000).toFixed(2),
        })),
    );
}

function parseRecentPower(raw: unknown): RecentPowerPoint[] {
  const recentMap = raw && typeof raw === 'object' ? (raw as Record<string, { time?: string; power?: number }>) : {};
  return Object.keys(recentMap)
    .sort()
    .map((key) => ({
      iso: String(recentMap[key]?.time || ''),
      power: +Number(recentMap[key]?.power ?? 0).toFixed(1),
    }))
    .filter((point) => point.iso);
}

function aggregateMonthly(points: DailyUsagePoint[]): TrendPoint[] {
  const totals = new Map<string, number>();
  for (const point of points) {
    const monthKey = point.dateKey.slice(0, 7);
    totals.set(monthKey, (totals.get(monthKey) || 0) + point.value);
  }

  return Array.from(totals.entries())
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([monthKey, value]) => ({
      label: `${monthKey.slice(5, 7)}/${monthKey.slice(2, 4)}`,
      tooltipLabel: `Tháng ${monthKey.slice(5, 7)}/${monthKey.slice(0, 4)}`,
      value: +value.toFixed(2),
    }));
}

function aggregateYearly(points: DailyUsagePoint[]): TrendPoint[] {
  const totals = new Map<string, number>();
  for (const point of points) {
    const yearKey = point.dateKey.slice(0, 4);
    totals.set(yearKey, (totals.get(yearKey) || 0) + point.value);
  }

  return Array.from(totals.entries())
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([yearKey, value]) => ({
      label: yearKey,
      tooltipLabel: `Năm ${yearKey}`,
      value: +value.toFixed(2),
    }));
}

function buildTrendModel(range: ChartRange, recentPower: RecentPowerPoint[], hourlyHistory: HourlyPoint[], dailyUsage: DailyUsagePoint[]): TrendModel {
  switch (range) {
    case 'hour':
      return {
        points: recentPower.map((point) => ({
          label: formatDateTime(point.iso, { hour: '2-digit', minute: '2-digit' }),
          tooltipLabel: formatDateTime(point.iso, {
            day: '2-digit',
            month: '2-digit',
            hour: '2-digit',
            minute: '2-digit',
            second: '2-digit',
          }),
          value: point.power,
        })),
        subtitle: '60 phút gần nhất · công suất tức thời · đơn vị W',
        emptyMessage: 'Chưa có dữ liệu 60 phút gần nhất ở nhánh power_recent',
        seriesLabel: 'Công suất',
        stroke: '#0ea5e9',
        fillId: 'trend-hour',
        yTickFormatter: (value) => `${Math.round(value)}W`,
        tooltipValueFormatter: (value) => `${value.toFixed(1)} W`,
      };
    case 'day': {
      const last24Hours = hourlyHistory.slice(-24);
      return {
        points: last24Hours.map((point) => ({
          label: formatDateTime(point.iso, { day: '2-digit', month: '2-digit', hour: '2-digit' }),
          tooltipLabel: formatDateTime(point.iso, {
            day: '2-digit',
            month: '2-digit',
            year: 'numeric',
            hour: '2-digit',
            minute: '2-digit',
          }),
          value: point.powerKw,
        })),
        subtitle: '24 giờ gần nhất · công suất theo từng giờ · đơn vị kW',
        emptyMessage: 'Chưa có đủ dữ liệu nhánh history cho 24 giờ gần nhất',
        seriesLabel: 'Công suất',
        stroke: '#2563eb',
        fillId: 'trend-day',
        yTickFormatter: (value) => `${value.toFixed(1)}kW`,
        tooltipValueFormatter: (value) => `${value.toFixed(2)} kW`,
      };
    }
    case 'month': {
      const last30Days = dailyUsage.slice(-30);
      return {
        points: last30Days.map((point) => ({
          label: `${point.dateKey.slice(8, 10)}/${point.dateKey.slice(5, 7)}`,
          tooltipLabel: point.dateKey,
          value: +point.value.toFixed(2),
        })),
        subtitle: '30 ngày gần nhất · điện năng theo ngày · đơn vị kWh',
        emptyMessage: 'Chưa có dữ liệu daily_usage cho 30 ngày gần nhất',
        seriesLabel: 'Điện năng',
        stroke: '#7c3aed',
        fillId: 'trend-month',
        yTickFormatter: (value) => `${value.toFixed(1)}kWh`,
        tooltipValueFormatter: (value) => `${value.toFixed(2)} kWh`,
      };
    }
    case 'year': {
      const last366Days = dailyUsage.slice(-366);
      return {
        points: aggregateMonthly(last366Days).slice(-12),
        subtitle: '12 tháng gần nhất · điện năng gộp theo tháng · đơn vị kWh',
        emptyMessage: 'Chưa có dữ liệu đủ dài để gộp 12 tháng gần nhất',
        seriesLabel: 'Điện năng',
        stroke: '#9333ea',
        fillId: 'trend-year',
        yTickFormatter: (value) => `${value.toFixed(0)}kWh`,
        tooltipValueFormatter: (value) => `${value.toFixed(2)} kWh`,
      };
    }
    case 'all':
    default: {
      const monthly = aggregateMonthly(dailyUsage);
      const points = monthly.length <= 24 ? monthly : aggregateYearly(dailyUsage);
      const byYear = monthly.length > 24;

      return {
        points,
        subtitle: byYear
          ? 'Toàn bộ lịch sử · điện năng gộp theo năm · đơn vị kWh'
          : 'Toàn bộ lịch sử · điện năng gộp theo tháng · đơn vị kWh',
        emptyMessage: 'Chưa có dữ liệu lịch sử trong nhánh daily_usage',
        seriesLabel: 'Điện năng',
        stroke: '#c026d3',
        fillId: 'trend-all',
        yTickFormatter: (value) => `${value.toFixed(0)}kWh`,
        tooltipValueFormatter: (value) => `${value.toFixed(2)} kWh`,
      };
    }
  }
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
    <div className={cn('stat-card flex flex-col justify-between group', emphasis && 'emphasis')}>
      <div className="mb-4 flex items-center justify-between">
        <p
          className={cn(
            'text-xs font-semibold tracking-wider uppercase',
            emphasis ? 'text-white/80' : 'text-on-surface-muted transition-colors duration-300 group-hover:text-primary',
          )}
        >
          {label}
        </p>
        {icon ? (
          <div className={cn('rounded-full p-2', emphasis ? 'bg-white/20 text-white' : 'bg-primary/10 text-primary')}>
            {icon}
          </div>
        ) : null}
      </div>
      <div className="flex items-baseline gap-1.5">
        <span className={cn('text-3xl font-bold tracking-tight', emphasis ? '' : 'text-on-surface')}>{value}</span>
        {unit ? <span className={cn('text-sm font-medium', emphasis ? 'text-white/80' : 'text-on-surface-muted')}>{unit}</span> : null}
      </div>
    </div>
  );
}

function ChartPanel({
  title,
  subtitle,
  actions,
  children,
}: {
  title: string;
  subtitle?: string;
  actions?: ReactNode;
  children: ReactNode;
}) {
  return (
    <section className="panel p-6 md:p-8">
      <div className="mb-6 flex flex-col justify-between gap-4 lg:flex-row lg:items-center">
        <div>
          <h2 className="font-headline text-xl font-bold text-on-surface">{title}</h2>
          {subtitle ? <p className="mt-1 text-[13px] text-on-surface-muted">{subtitle}</p> : null}
        </div>
        {actions ? <div className="flex flex-wrap gap-2">{actions}</div> : null}
      </div>
      {children}
    </section>
  );
}

export default function App() {
  const [realtimeData, setRealtimeData] = useState<RealtimeState>({
    metrics: EMPTY_METRICS,
    consumption: EMPTY_CONSUMPTION,
    timestamp: null,
  });
  const [recentPower, setRecentPower] = useState<RecentPowerPoint[]>([]);
  const [hourlyHistory, setHourlyHistory] = useState<HourlyPoint[]>([]);
  const [dailyUsage, setDailyUsage] = useState<DailyUsagePoint[]>([]);
  const [selectedRange, setSelectedRange] = useState<ChartRange>('day');

  const [socketConnected, setSocketConnected] = useState(false);
  const [readError, setReadError] = useState<string | null>(null);
  const [connectTimeout, setConnectTimeout] = useState(false);

  useEffect(() => {
    if (!firebaseConfigured) return;

    const onDenied = (err: Error) => setReadError(err.message);

    const unsubSocket = onValue(ref(db, '.info/connected'), (snap) => setSocketConnected(snap.val() === true), onDenied);

    const unsubRealtime = onValue(
      ref(db, 'realtime'),
      (snapshot) => {
        if (snapshot.exists()) setRealtimeData(normalizeRealtime(snapshot.val()));
      },
      onDenied,
    );

    const unsubRecentPower = onValue(
      query(ref(db, 'power_recent'), orderByKey(), limitToLast(720)),
      (snapshot) => {
        setRecentPower(snapshot.exists() ? parseRecentPower(snapshot.val()) : []);
      },
      onDenied,
    );

    const unsubHistory = onValue(
      query(ref(db, 'history'), orderByKey(), limitToLast(2)),
      (snapshot) => {
        setHourlyHistory(snapshot.exists() ? parseHourlyHistory(snapshot.val()) : []);
      },
      onDenied,
    );

    const unsubUsage = onValue(
      query(ref(db, 'daily_usage'), orderByKey()),
      (snapshot) => {
        setDailyUsage(snapshot.exists() ? parseDailyUsage(snapshot.val()) : []);
      },
      onDenied,
    );

    const timeoutId = window.setTimeout(() => {
      setConnectTimeout(true);
    }, 10000);

    return () => {
      window.clearTimeout(timeoutId);
      unsubSocket();
      unsubRealtime();
      unsubRecentPower();
      unsubHistory();
      unsubUsage();
    };
  }, []);

  const trend = buildTrendModel(selectedRange, recentPower, hourlyHistory, dailyUsage);

  const lastUpdated =
    realtimeData.timestamp &&
    formatDateTime(realtimeData.timestamp, {
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
              <p className="text-xs text-on-surface-muted">Một biểu đồ động cho mọi mốc thời gian</p>
            </div>
          </div>

          <div
            className={cn(
              'flex items-start gap-2 rounded-xl border px-3 py-2.5 text-sm',
              status.tone === 'ok' && 'border-transparent bg-ok-bg text-ok',
              status.tone === 'warn' && 'border-transparent bg-warn-bg text-warn',
              status.tone === 'danger' && 'border-transparent bg-danger-bg text-danger',
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
                  Tạo <code className="rounded bg-surface px-1 py-0.5 font-mono text-xs">kinetic-precision/.env</code> từ{' '}
                  <code className="rounded bg-surface px-1 py-0.5 font-mono text-xs">.env.example</code>.
                </li>
                <li>
                  Đặt <code className="rounded bg-surface px-1 py-0.5 font-mono text-xs">VITE_FIREBASE_DATABASE_URL</code> = URL Realtime Database
                  hợp lệ.
                </li>
                <li>Điền thêm các biến <span className="font-mono">VITE_FIREBASE_*</span> từ Firebase Console.</li>
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
          <Stat label="Tiền hôm nay" value={Math.max(0, realtimeData.consumption.daily_cost).toLocaleString('vi-VN')} unit="đ" />
          <Stat
            label="Tiền tháng (tạm tính)"
            value={Math.max(0, realtimeData.consumption.monthly_cost).toLocaleString('vi-VN')}
            unit="đ"
            emphasis
            icon={<Wallet className="h-4 w-4" />}
          />
        </section>

        <ChartPanel
          title="Xu hướng tiêu thụ"
          subtitle={trend.subtitle}
          actions={RANGE_OPTIONS.map((option) => (
            <button
              key={option.key}
              type="button"
              onClick={() => setSelectedRange(option.key)}
              className={cn(
                'rounded-full border px-3 py-1.5 text-sm font-medium transition-colors',
                selectedRange === option.key
                  ? 'border-primary bg-primary text-white shadow-sm'
                  : 'border-[var(--color-border)] bg-white/70 text-on-surface-muted hover:border-primary/40 hover:text-on-surface',
              )}
            >
              {option.label}
            </button>
          ))}
        >
          <div className="h-[360px] w-full">
            <ResponsiveContainer width="100%" height="100%">
              {trend.points.length > 0 ? (
                <AreaChart data={trend.points} margin={{ top: 8, right: 10, left: 0, bottom: 0 }}>
                  <defs>
                    <linearGradient id={trend.fillId} x1="0" y1="0" x2="0" y2="1">
                      <stop offset="0%" stopColor={trend.stroke} stopOpacity={0.24} />
                      <stop offset="100%" stopColor={trend.stroke} stopOpacity={0} />
                    </linearGradient>
                  </defs>
                  <CartesianGrid strokeDasharray="4 4" vertical={false} stroke="var(--color-border)" />
                  <XAxis dataKey="label" axisLine={false} tickLine={false} tick={{ fontSize: 11, fill: '#5c6370' }} dy={8} minTickGap={24} />
                  <YAxis
                    width={56}
                    axisLine={false}
                    tickLine={false}
                    tick={{ fontSize: 11, fill: '#5c6370' }}
                    tickFormatter={trend.yTickFormatter}
                  />
                  <Tooltip
                    labelFormatter={(_, payload) => String(payload?.[0]?.payload?.tooltipLabel || '')}
                    formatter={(value: number) => [trend.tooltipValueFormatter(Number(value)), trend.seriesLabel]}
                    contentStyle={{
                      borderRadius: '10px',
                      border: '1px solid var(--color-border)',
                      fontSize: '12px',
                    }}
                  />
                  <Area type="monotone" dataKey="value" stroke={trend.stroke} strokeWidth={2} fill={`url(#${trend.fillId})`} />
                </AreaChart>
              ) : (
                <div className="flex h-full flex-col items-center justify-center gap-2 text-center text-sm text-on-surface-muted">
                  <Activity className="h-8 w-8 opacity-40" />
                  <p>{trend.emptyMessage}</p>
                </div>
              )}
            </ResponsiveContainer>
          </div>
        </ChartPanel>
      </div>
    </div>
  );
}
