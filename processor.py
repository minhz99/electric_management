# processor.py
"""
Module xử lý dữ liệu MQTT từ ESP8266 PZEM004T và đồng bộ lên Firebase.
Tự động khôi phục state khi server restart.
Đã tích hợp ThreadPoolExecutor để I/O không block MQTT hook.
"""

import json
import logging
import concurrent.futures
from datetime import datetime, timedelta
import time
from typing import Dict

STATE_SAVE_INTERVAL_SEC = 300  # Lưu state tối đa mỗi 5 phút
REALTIME_PUSH_INTERVAL_SEC = 10.0  # Throttle độ trễ đẩy lên Realtime (10 giây)
ENERGY_ROLLBACK_TOLERANCE_KWH = 0.001
HISTORY_RETENTION_DAYS = 1095   # 3 năm dữ liệu theo giờ
DAILY_USAGE_RETENTION_DAYS = 3650  # 10 năm dữ liệu tổng theo ngày
RECENT_POWER_RETENTION_SECONDS = 3600  # 60 phút gần nhất cho biểu đồ "giờ"
RECENT_POWER_CLEANUP_INTERVAL = 12  # dọn 1 lần/phút nếu ESP gửi mỗi 5 giây

from config import (
    init_firebase,
    init_mqtt,
    TIMEZONE_GMT7,
    PZEM_ENERGY_OFFSET_KWH,
)

STATE_COORD_VERSION = "adjusted_v1"
from pricing import calc_electricity_cost

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ElectricityProcessor:
    def __init__(self):
        """Khởi tạo processor với kết nối Firebase và MQTT"""
        self.firebase_ref = init_firebase()
        if not self.firebase_ref:
            raise RuntimeError("Không thể khởi động do lỗi kết nối Firebase.")
            
        self.state_ref = self.firebase_ref.child('state') if self.firebase_ref else None
        self.realtime_ref = self.firebase_ref.child('realtime') if self.firebase_ref else None
        self.history_ref = self.firebase_ref.child('history') if self.firebase_ref else None
        self.power_recent_ref = self.firebase_ref.child('power_recent') if self.firebase_ref else None

        self.last_energy_reading = None
        self.monthly_start_energy = None
        self.daily_start_energy = None

        self.last_history_slot = None
        self.last_state_save_time = 0.0  # throttle: save state tối đa mỗi 5 phút
        self.last_realtime_push_time = 0.0 # throttle realtime/daily_usage push
        self.last_processed_date = None
        self.last_processed_month_key = None
        self.recent_power_writes_since_cleanup = 0
        
        # Sử dụng Thread Pool để xử lý bất đồng bộ các request HTTP chặn (Firebase Admin)
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=4)

        # Khởi tạo giá trị ban đầu từ Firebase
        self._initialize_energy_baseline()

        logger.info("Đã khởi tạo ElectricityProcessor.")
    
    @staticmethod
    def _to_adjusted_kwh(raw_energy: float) -> float:
        """Chuyển kWh thô từ PZEM sang kWh quy chiếu (đã trừ offset công tơ cũ)."""
        return max(0.0, raw_energy - PZEM_ENERGY_OFFSET_KWH)

    def _state_coordinate_ok(self, state: dict) -> bool:
        if state.get("coord_version") != STATE_COORD_VERSION:
            return False
        try:
            saved_off = float(state.get("pzem_energy_offset_kwh", -1.0))
        except (TypeError, ValueError):
            return False
        return abs(saved_off - PZEM_ENERGY_OFFSET_KWH) < 1e-6

    def _initialize_energy_baseline(self):
        """Khôi phục baseline từ Firebase; bù ngày/tháng nếu server tắt qua ranh giới."""
        if not self.state_ref:
            return

        try:
            state = self.state_ref.get()
            now = datetime.now(TIMEZONE_GMT7)
            today_str = now.strftime("%Y-%m-%d")
            current_month_key = now.strftime("%Y-%m")

            if not state:
                logger.info("Node state trống trên Firebase, sẽ tạo state mới ở lần nhận dữ liệu đầu tiên.")
                return

            if not self._state_coordinate_ok(state):
                logger.info(
                    "State cũ hoặc đổi PZEM_ENERGY_OFFSET_KWH: khởi tạo lại baseline từ mẫu điện tiếp theo."
                )
                return

            saved_date = state.get("last_date", "")
            saved_month = int(state.get("last_month", 0) or 0)
            saved_month_key = state.get("last_month_key") or (saved_date[:7] if saved_date else "")
            if not saved_month_key and saved_month:
                saved_month_key = f"{now.year:04d}-{saved_month:02d}"

            self.last_energy_reading = float(state.get("last_energy", 0.0))
            self.monthly_start_energy = float(state.get("monthly_start_energy", 0.0))
            self.daily_start_energy = float(state.get("daily_start_energy", 0.0))

            if saved_month_key != current_month_key:
                logger.info("Qua tháng mới khi server tắt — baseline tháng = chỉ số cuối đã lưu.")
                self.monthly_start_energy = self.last_energy_reading

            if saved_date != today_str:
                logger.info("Qua ngày mới khi server tắt — baseline ngày = chỉ số cuối đã lưu.")
                self.daily_start_energy = self.last_energy_reading

            self.last_processed_date = today_str
            self.last_processed_month_key = current_month_key

            logger.info("Khôi phục state từ Firebase thành công:")
            logger.info(f"  - Energy cuối (đã trừ offset): {self.last_energy_reading} kWh")
            logger.info(f"  - Baseline tháng: {self.monthly_start_energy} kWh")
            logger.info(f"  - Baseline ngày: {self.daily_start_energy} kWh")

        except Exception as e:
            logger.error(f"Lỗi khởi tạo energy baseline từ Firebase: {e}")

    def _cleanup_retention(self, now: datetime):
        """Xóa mốc cũ theo ngày để giữ dữ liệu dài hạn nhưng không tăng vô hạn."""
        history_expired = (now - timedelta(days=HISTORY_RETENTION_DAYS)).strftime("%Y-%m-%d")
        usage_expired = (now - timedelta(days=DAILY_USAGE_RETENTION_DAYS)).strftime("%Y-%m-%d")

        def worker():
            try:
                if self.history_ref:
                    self.history_ref.child(history_expired).delete()
                if self.firebase_ref:
                    self.firebase_ref.child('daily_usage').child(usage_expired).delete()
            except Exception as e:
                logger.warning(f"Lỗi dọn retention Firebase: {e}")
        self.executor.submit(worker)

    def _handle_period_rollover(self, now: datetime):
        """Cập nhật baseline khi bản tin đầu tiên đi qua ranh giới ngày/tháng theo GMT+7."""
        current_date = now.strftime("%Y-%m-%d")
        current_month_key = now.strftime("%Y-%m")

        if self.last_energy_reading is None:
            self.last_processed_date = current_date
            self.last_processed_month_key = current_month_key
            return

        day_changed = self.last_processed_date not in (None, current_date)
        month_changed = self.last_processed_month_key not in (None, current_month_key)

        if month_changed or self.monthly_start_energy is None:
            self.monthly_start_energy = self.last_energy_reading
            logger.info(f"Qua tháng mới — baseline tháng = {self.monthly_start_energy} kWh (đã trừ offset)")

        if day_changed or self.daily_start_energy is None:
            self.daily_start_energy = self.last_energy_reading
            logger.info(f"Qua ngày mới — baseline ngày = {self.daily_start_energy} kWh (đã trừ offset)")
            self._cleanup_retention(now)

        self.last_processed_date = current_date
        self.last_processed_month_key = current_month_key

    def _save_state(self, current_energy: float, force: bool = False):
        """Lưu state lên Firebase, throttle tối đa mỗi STATE_SAVE_INTERVAL_SEC giây."""
        if not self.state_ref:
            return
        now_ts = time.monotonic()
        if not force and (now_ts - self.last_state_save_time) < STATE_SAVE_INTERVAL_SEC:
            return
            
        now = datetime.now(TIMEZONE_GMT7)
        state_data = {
            "last_energy": current_energy,
            "monthly_start_energy": self.monthly_start_energy,
            "daily_start_energy": self.daily_start_energy,
            "last_date": now.strftime("%Y-%m-%d"),
            "last_month": now.month,
            "last_month_key": now.strftime("%Y-%m"),
            "last_updated": now.isoformat(),
            "coord_version": STATE_COORD_VERSION,
            "pzem_energy_offset_kwh": PZEM_ENERGY_OFFSET_KWH,
        }
        self.last_state_save_time = now_ts
        
        def worker():
            try:
                self.state_ref.update(state_data)
            except Exception as e:
                logger.error(f"Lỗi ghi state lên Firebase: {e}")
        self.executor.submit(worker)

    def _push_recent_power(self, power_w: float, now: datetime):
        """Giữ 60 phút công suất gần nhất để chart "giờ" có dữ liệu sau khi reload UI."""
        if not self.power_recent_ref:
            return
            
        timestamp_ms = int(now.timestamp() * 1000)
        self.recent_power_writes_since_cleanup += 1
        do_cleanup = (self.recent_power_writes_since_cleanup >= RECENT_POWER_CLEANUP_INTERVAL)
        if do_cleanup:
            self.recent_power_writes_since_cleanup = 0
            
        def worker():
            try:
                self.power_recent_ref.child(str(timestamp_ms)).set({
                    "time": now.isoformat(),
                    "power": round(power_w, 1),
                })
                
                if do_cleanup:
                    cutoff_ms = int((now - timedelta(seconds=RECENT_POWER_RETENTION_SECONDS)).timestamp() * 1000)
                    snap = self.power_recent_ref.get()
                    if not snap:
                        return
                    for key in snap.keys():
                        try:
                            if int(key) < cutoff_ms:
                                self.power_recent_ref.child(key).delete()
                        except (TypeError, ValueError):
                            self.power_recent_ref.child(key).delete()
            except Exception as e:
                logger.warning(f"Lỗi lưu power_recent: {e}")
                
        self.executor.submit(worker)

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
            
            raw_energy = float(data.get("energy", 0))
            if raw_energy < 0:
                return
            energy = self._to_adjusted_kwh(raw_energy)

            if self.last_energy_reading is not None:
                if energy + ENERGY_ROLLBACK_TOLERANCE_KWH < self.last_energy_reading:
                    logger.warning(
                        "Bỏ qua mẫu MQTT vì energy đi lùi: raw=%.3f adjusted=%.3f last=%.3f",
                        raw_energy,
                        energy,
                        self.last_energy_reading,
                    )
                    return
                if energy < self.last_energy_reading:
                    energy = self.last_energy_reading

            if self.last_energy_reading is None:
                self.daily_start_energy = energy
                self.monthly_start_energy = energy
                self.last_processed_date = today_str
                self.last_processed_month_key = now.strftime("%Y-%m")
            else:
                self._handle_period_rollover(now)

            # Tính toán tiêu thụ (energy & baseline trong DB đều là kWh sau offset)
            monthly_consumption = max(0.0, energy - float(self.monthly_start_energy))
            daily_consumption = max(0.0, energy - float(self.daily_start_energy))
            
            monthly_cost = calc_electricity_cost(monthly_consumption)
            
            yesterday_consumption = max(0, monthly_consumption - daily_consumption)
            yesterday_cost = calc_electricity_cost(yesterday_consumption)
            
            daily_cost_value = monthly_cost['total'] - yesterday_cost['total']
            
            self.last_energy_reading = energy
            self._save_state(energy)  # throttled — tự động bỏ qua nếu chưa đủ 5 phút

            power_w = float(data.get('power', 0))

            # 1. Realtime node & Daily Usage (luôn cập nhật nhưng có Throttle giới hạn)
            now_ts = time.monotonic()
            if now_ts - self.last_realtime_push_time >= REALTIME_PUSH_INTERVAL_SEC:
                self.last_realtime_push_time = now_ts
                
                def write_realtime():
                    try:
                        if self.realtime_ref:
                            self.realtime_ref.set({
                                "metrics": {
                                    "voltage":   float(data.get('voltage', 0)),
                                    "current":   float(data.get('current', 0)),
                                    "power":     power_w,
                                    "frequency": float(data.get('frequency', 0)),
                                    "pf":        float(data.get('pf', 0)),
                                },
                                "consumption": {
                                    "daily_kwh":    daily_consumption,
                                    "monthly_kwh":  monthly_consumption,
                                    "total_kwh":    energy,
                                    "daily_cost":   daily_cost_value,
                                    "monthly_cost": monthly_cost['total'],
                                },
                                "timestamp": now.isoformat(),
                            })
                            
                        # 2. Ngầm lưu daily usage định kỳ theo nhánh realtime (hạn chế fetch/ghim database tần suất cao)
                        if self.firebase_ref:
                            self.firebase_ref.child('daily_usage').child(today_str).set(daily_consumption)
                    except Exception as e:
                        logger.error(f"Lỗi ghi Realtime Firebase: {e}")
                        
                self.executor.submit(write_realtime)

            # 3. Công suất gần nhất cho chart range "giờ"
            # Cần ghi đè liên tục để biểu đồ giờ có đủ history điểm nếu UI vừa f5. 
            # Đã có executor Thread bên trong xử lý.
            self._push_recent_power(power_w, now)

            # 4. Lịch sử theo giờ
            history_slot = f"{today_str}T{now.hour:02d}"
            if self.history_ref and history_slot != self.last_history_slot:
                hour_str = f"{now.hour:02d}:00"
                self.last_history_slot = history_slot
                
                def write_history():
                    try:
                        self.history_ref.child(today_str).child(hour_str).set({
                            "power": power_w,
                            "energy_accumulated": daily_consumption,
                        })
                    except Exception as e:
                        logger.error(f"Lỗi ghi History Firebase: {e}")
                self.executor.submit(write_history)

            logger.info(
                f"P={power_w}W  adj_E={energy:.3f}kWh  Daily={daily_consumption:.3f}kWh  Monthly={monthly_consumption:.3f}kWh"
            )
            
        except Exception as e:
            logger.error(f"Lỗi xử lý Data: {e}")
            
    def run(self):
        try:
            mqtt_client = init_mqtt(self.process_mqtt_message)
            mqtt_client.loop_start()
            logger.info("ElectricityProcessor (Firebase mode) đang chạy (pool: 4 threads)...")
            
            while True:
                time.sleep(1)
                
        except KeyboardInterrupt:
            logger.info("Đang dừng...")
            mqtt_client.loop_stop()
            mqtt_client.disconnect()
            self.executor.shutdown(wait=True)
        except Exception as e:
            logger.error(f"Lỗi trong main loop: {e}")
            self.executor.shutdown(wait=False)
            raise

if __name__ == "__main__":
    processor = ElectricityProcessor()
    processor.run()
