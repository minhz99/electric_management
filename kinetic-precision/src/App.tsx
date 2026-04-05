/**
 * @license
 * SPDX-License-Identifier: Apache-2.0
 */

import { useState, useMemo, useEffect } from 'react';
import { 
  Bell, 
  Home, 
  History, 
  Cpu, 
  Settings, 
  ArrowUp, 
  CheckCircle2, 
  Wallet,
  MoreHorizontal
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
import { db } from './firebase';

function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
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
  icon?: React.ReactNode;
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

export default function App() {
  const [powerFilter, setPowerFilter] = useState('Ngày');
  const [usageFilter, setUsageFilter] = useState('Ngày');

  const [realtimeData, setRealtimeData] = useState({
    metrics: { voltage: 0, current: 0, power: 0, frequency: 0, pf: 0 },
    consumption: { daily_kwh: 0, monthly_kwh: 0, daily_cost: 0, monthly_cost: 0 }
  });
  
  const [powerHistory, setPowerHistory] = useState<any[]>([]);
  const [dailyUsage, setDailyUsage] = useState<any[]>([]);

  useEffect(() => {
    // Lắng nghe dữ liệu thời gian thực
    const rtRef = ref(db, 'realtime');
    const unsubRt = onValue(rtRef, (snapshot) => {
      if(snapshot.exists()) {
        setRealtimeData(snapshot.val());
      }
    });
     
    // Lắng nghe dữ liệu lịch sử CS (Biểu đồ 1)
    const historyRef = ref(db, 'history');
    const unsubHistory = onValue(historyRef, (snapshot) => {
      if(snapshot.exists()) {
        const historyMap = snapshot.val();
        // Lấy ngày hôm nay theo giờ VN
        const offset = new Date().getTimezoneOffset() == 0 ? 7 * 60 * 60000 : 0; 
        const todayStr = new Date(Date.now() + offset).toISOString().split('T')[0];
        
        const todayHistory = historyMap[todayStr] || {};
        const formatted = Object.keys(todayHistory).sort().map(time => ({
          time: time,
          power: +(todayHistory[time].power / 1000).toFixed(2) // Convert W to kW
        }));
        setPowerHistory(formatted);
      }
    });

    // Lắng nghe lượng điện sử dụng theo ngày (Biểu đồ 2)
    const usageRef = ref(db, 'daily_usage');
    const unsubUsage = onValue(usageRef, (snapshot) => {
      if(snapshot.exists()) {
        const usageMap = snapshot.val();
        const formatted = Object.keys(usageMap).sort().slice(-7).map(date => ({
          day: date.substring(5), // Lấy MM-DD
          value: +(usageMap[date]).toFixed(2)
        }));
        setDailyUsage(formatted);
      }
    });

    return () => {
      unsubRt();
      unsubHistory();
      unsubUsage();
    };
  }, []);

  return (
    <div className="min-h-screen kinetic-grid pb-20 md:pb-0">
      {/* Header */}
      <header className="w-full sticky top-0 flex justify-between items-center px-6 md:px-8 py-4 bg-white/80 backdrop-blur-md z-40 border-b border-outline-variant/10">
        <div className="flex items-center gap-8">
          <span className="text-xl font-black text-on-surface font-headline">Kinetic Precision</span>
          <nav className="hidden md:flex items-center space-x-6">
            <a className="flex items-center gap-2 text-primary font-bold text-sm" href="#">
              <Home className="w-4 h-4 fill-current" />
              <span>Trang Chủ</span>
            </a>
            <a className="flex items-center gap-2 text-on-surface-variant hover:text-primary transition-colors text-sm font-medium" href="#">
              <History className="w-4 h-4" />
              <span>Lịch Sử</span>
            </a>
            <a className="flex items-center gap-2 text-on-surface-variant hover:text-primary transition-colors text-sm font-medium" href="#">
              <Cpu className="w-4 h-4" />
              <span>Thiết Bị</span>
            </a>
          </nav>
        </div>
        
        <div className="flex items-center gap-6">
          <button className="text-on-surface-variant hover:text-primary transition-colors">
            <Bell className="w-5 h-5" />
          </button>
          <div className="h-8 w-px bg-outline-variant/20"></div>
          <div className="flex items-center gap-3 cursor-pointer group">
            <img 
              alt="User profile" 
              className="w-8 h-8 rounded-full object-cover ring-2 ring-transparent group-hover:ring-primary/20 transition-all" 
              src="https://picsum.photos/seed/alex/100/100"
              referrerPolicy="no-referrer"
            />
            <span className="hidden md:inline text-on-surface font-bold font-headline text-sm">Alex Rivera</span>
          </div>
        </div>
      </header>

      <main className="max-w-7xl mx-auto p-6 md:p-8 space-y-8">
        {/* Metric Cards Grid */}
        <section className="grid grid-cols-1 sm:grid-cols-2 md:grid-cols-3 lg:grid-cols-5 gap-4">
          <MetricCard 
            label="Điện Áp (V)" 
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
          className="bg-surface-container-low rounded-2xl p-6 md:p-8 border border-outline-variant/20"
        >
          <div className="flex flex-col md:flex-row justify-between items-start md:items-center gap-4 mb-10">
            <div>
              <h3 className="text-2xl font-bold font-headline text-on-surface">Công suất điện theo thời gian</h3>
              <p className="text-sm text-on-surface-variant">Theo dõi trực tiếp tải tiêu thụ tại nút (kW)</p>
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
                <div className="flex items-center justify-center h-full text-on-surface-variant">Chưa có dữ liệu lịch sử</div>
              )}
            </ResponsiveContainer>
          </div>
        </motion.section>

        {/* Usage Chart Section */}
        <motion.section 
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.3 }}
          className="bg-surface-container-low rounded-2xl p-6 md:p-8 border border-outline-variant/20"
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
