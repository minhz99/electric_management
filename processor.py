# processor.py
"""
Module xử lý dữ liệu MQTT từ ESP8266 PZEM004T và đồng bộ lên Firebase.
Tự động khôi phục state khi server restart.
"""

import json
import logging
from datetime import datetime
import time
from typing import Dict
import schedule

from config import init_firebase, init_mqtt, TIMEZONE_GMT7
from pricing import calc_electricity_cost

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ElectricityProcessor:
    def __init__(self):
        """Khởi tạo processor với kết nối Firebase và MQTT"""
        self.firebase_ref = init_firebase()
        if not self.firebase_ref:
            logger.error("Không thể khởi động do lỗi kết nối Firebase.")
            # Core functions will skip saving if ref is None
            
        self.state_ref = self.firebase_ref.child('state') if self.firebase_ref else None
        self.realtime_ref = self.firebase_ref.child('realtime') if self.firebase_ref else None
        self.history_ref = self.firebase_ref.child('history') if self.firebase_ref else None
        
        self.last_energy_reading = None
        self.monthly_start_energy = 0.0
        self.daily_start_energy = 0.0
        
        self.last_save_hour = -1
        
        # Khởi tạo giá trị ban đầu từ Firebase
        self._initialize_energy_baseline()
        
        schedule.every().day.at("00:00").do(self._reset_daily_energy)
        
        logger.info("Đã khởi tạo ElectricityProcessor.")
    
    def _initialize_energy_baseline(self):
        """Khởi tạo baseline energy cho tháng/ngày hiện tại từ Firebase state"""
        if not self.state_ref:
            return
            
        try:
            state = self.state_ref.get()
            now = datetime.now(TIMEZONE_GMT7)
            today_str = now.strftime('%Y-%m-%d')
            current_month = now.month
            
            if state:
                saved_date = state.get('last_date', '')
                saved_month = state.get('last_month', 0)
                
                self.last_energy_reading = state.get('last_energy', 0)
                self.monthly_start_energy = state.get('monthly_start_energy', 0.0)
                self.daily_start_energy = state.get('daily_start_energy', 0.0)
                
                # Cập nhật logic qua ngày/tháng mới khi server đang tắt
                if saved_month != current_month:
                    logger.info("Phát hiện qua tháng mới trong lúc server tắt. Baseline tháng sẽ chờ PZEM reset.")
                
                if saved_date != today_str:
                    logger.info("Phát hiện qua ngày mới trong lúc server tắt. Reset daily baseline.")
                    self.daily_start_energy = self.last_energy_reading if self.last_energy_reading else 0.0
                
                logger.info("Khôi phục state từ Firebase thành công:")
                logger.info(f"  - Energy cuối: {self.last_energy_reading} kWh")
                logger.info(f"  - Baseline tháng: {self.monthly_start_energy} kWh")
                logger.info(f"  - Baseline ngày: {self.daily_start_energy} kWh")
            else:
                logger.info("Node state trống trên Firebase, sẽ tạo state mới ở lần nhận dữ liệu đầu tiên.")
                
        except Exception as e:
            logger.error(f"Lỗi khởi tạo energy baseline từ Firebase: {e}")
    
    def _reset_daily_energy(self):
        """Reset daily energy baseline (called by schedule)"""
        if self.last_energy_reading is not None:
            self.daily_start_energy = self.last_energy_reading
            self._save_state(self.last_energy_reading)
            logger.info(f"Đã reset daily baseline qua ngày mới: {self.daily_start_energy} kWh")

    def _save_state(self, current_energy: float):
        """Lưu trạng thái state lên Firebase để phục hồi chống mất dữ liệu"""
        if not self.state_ref:
            return
            
        try:
            now = datetime.now(TIMEZONE_GMT7)
            state_data = {
                "last_energy": current_energy,
                "monthly_start_energy": self.monthly_start_energy,
                "daily_start_energy": self.daily_start_energy,
                "last_date": now.strftime('%Y-%m-%d'),
                "last_month": now.month,
                "last_updated": now.isoformat()
            }
            self.state_ref.update(state_data)
        except Exception as e:
            logger.error(f"Lỗi ghi state lên Firebase: {e}")

    def process_mqtt_message(self, client, userdata, message):
        """Xử lý tin nhắn MQTT từ ESP8266"""
        try:
            payload = message.payload.decode('utf-8')
            data = json.loads(payload)
            
            if all(key in data for key in ['voltage', 'current', 'power', 'energy', 'frequency', 'pf']):
                self._process_pzem_data(data)
                
        except json.JSONDecodeError as e:
            pass # Ignore malformed
        except Exception as e:
            logger.error(f"Lỗi xử lý MQTT message: {e}")
    
    def _process_pzem_data(self, data: Dict):
        """Xử lý dữ liệu và tính toán"""
        try:
            now = datetime.now(TIMEZONE_GMT7)
            today_str = now.strftime('%Y-%m-%d')
            
            energy = float(data.get('energy', 0))
            if energy < 0:
                return
            
            # Logic: Khi PZEM tự reset (đầu tháng) -> energy tụt mạnh
            if self.last_energy_reading is not None and energy < self.last_energy_reading * 0.5:
                logger.info(f"PZEM Reset detected: energy from {self.last_energy_reading} -> {energy}")
                self.monthly_start_energy = energy
                self.daily_start_energy = energy
            
            if self.last_energy_reading is None:
                self.daily_start_energy = energy
                self.last_energy_reading = energy

            # Tính toán tiêu thụ
            monthly_consumption = max(0, energy - self.monthly_start_energy)
            daily_consumption = max(0, energy - self.daily_start_energy)
            
            monthly_cost = calc_electricity_cost(monthly_consumption)
            
            yesterday_consumption = max(0, monthly_consumption - daily_consumption)
            yesterday_cost = calc_electricity_cost(yesterday_consumption)
            
            daily_cost_value = monthly_cost['total'] - yesterday_cost['total']
            
            self.last_energy_reading = energy
            self._save_state(energy)
            
            # 2. Lưu lên Node Realtime Firebase
            if self.realtime_ref:
                realtime_data = {
                    "metrics": {
                        "voltage": float(data.get('voltage', 0)),
                        "current": float(data.get('current', 0)),
                        "power": float(data.get('power', 0)),
                        "frequency": float(data.get('frequency', 0)),
                        "pf": float(data.get('pf', 0))
                    },
                    "consumption": {
                        "daily_kwh": daily_consumption,
                        "monthly_kwh": monthly_consumption,
                        "daily_cost": daily_cost_value,
                        "monthly_cost": monthly_cost['total']
                    },
                    "timestamp": now.isoformat()
                }
                self.realtime_ref.set(realtime_data)
            
            # 3. Lưu lịch sử mỗi giờ 1 lần 
            if self.history_ref and now.hour != self.last_save_hour:
                hour_str = f"{now.hour:02d}:00"
                self.history_ref.child(today_str).child(hour_str).set({
                    "power": float(data.get('power', 0)),
                    "energy_accumulated": daily_consumption
                })
                self.last_save_hour = now.hour

            # Lưu daily_usage cho chart
            if self.firebase_ref:
                self.firebase_ref.child('daily_usage').child(today_str).set(daily_consumption)
            
            logger.info(f"Updated Firebase: P={data.get('power')}W, E={energy}kWh, Daily={daily_consumption:.3f}kWh, Monthly={monthly_consumption:.3f}kWh")
            
        except Exception as e:
            logger.error(f"Lỗi lưu Firebase: {e}")
            
    def run(self):
        try:
            mqtt_client = init_mqtt(self.process_mqtt_message)
            mqtt_client.loop_start()
            logger.info("ElectricityProcessor (Firebase mode) đang chạy...")
            
            while True:
                schedule.run_pending()
                time.sleep(1)
                
        except KeyboardInterrupt:
            logger.info("Đang dừng...")
            mqtt_client.loop_stop()
            mqtt_client.disconnect()
        except Exception as e:
            logger.error(f"Lỗi trong main loop: {e}")
            raise

if __name__ == "__main__":
    processor = ElectricityProcessor()
    processor.run()
