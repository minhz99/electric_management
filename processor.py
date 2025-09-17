# processor.py
"""
Module xử lý dữ liệu MQTT từ ESP8266 PZEM004T và gửi vào InfluxDB
Tính toán điện tiêu thụ hàng ngày/tháng và dự đoán
"""

import json
import logging
from datetime import datetime, timedelta
import schedule
import time
from typing import Dict

from config import init_influx, write_influx, init_mqtt, INFLUX_BUCKET, INFLUX_ORG, DAILY_RESET_TIME, MONTH_START_DAY, TIMEZONE_GMT7
from pricing import calc_electricity_cost

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class ElectricityProcessor:
    def __init__(self):
        """Khởi tạo processor với kết nối InfluxDB và MQTT"""
        self.influx_client = init_influx()
        logger.info("Đã kết nối InfluxDB thành công")
        
        self.last_energy_reading = None
        self.monthly_start_energy = None
        self.daily_start_energy = None
        
        # Health monitoring
        self.last_data_time = None
        self.mqtt_connected = False
        self.influx_healthy = True
        
        # Khởi tạo giá trị ban đầu
        self._initialize_energy_baseline()
        
        # Schedule job lúc DAILY_RESET_TIME mỗi ngày. Trong job sẽ reset daily
        # và nếu là ngày đầu tháng thì reset monthly.
        schedule.every().day.at(DAILY_RESET_TIME).do(self._midnight_job)
        
        logger.info("Đã khởi tạo ElectricityProcessor")
    
    def _initialize_energy_baseline(self):
        """Khởi tạo baseline energy cho tháng/ngày hiện tại từ InfluxDB"""
        try:
            query_api = self.influx_client.query_api()
            
            # 1. Lấy giá trị energy gần nhất
            self.last_energy_reading = self._get_latest_energy_from_db(query_api)
            
            # 2. Lấy baseline đầu tháng (ngày MONTH_START_DAY lúc 00:00)
            self.monthly_start_energy = self._get_month_start_energy_from_db(query_api)
            
            # 3. Lấy baseline đầu ngày hôm nay (00:00)
            self.daily_start_energy = self._get_daily_start_energy_from_db(query_api)
            
            # Kiểm tra và áp dụng fallback logic nếu cần
            self._apply_fallback_logic()
            
            logger.info(f"Khôi phục baseline thành công:")
            logger.info(f"  - Energy hiện tại: {self.last_energy_reading} kWh")
            logger.info(f"  - Baseline đầu tháng: {self.monthly_start_energy} kWh")
            logger.info(f"  - Baseline đầu ngày: {self.daily_start_energy} kWh")
            
            # Tính toán và hiển thị điện tiêu thụ hiện tại
            daily_consumption = max(0, self.last_energy_reading - self.daily_start_energy)
            monthly_consumption = max(0, self.last_energy_reading - self.monthly_start_energy)
            logger.info(f"  - Điện tiêu thụ hôm nay: {daily_consumption:.3f} kWh")
            logger.info(f"  - Điện tiêu thụ tháng này: {monthly_consumption:.3f} kWh")
            
        except Exception as e:
            logger.error(f"Lỗi khởi tạo energy baseline: {e}")
            self.last_energy_reading = 0
            self.monthly_start_energy = 0
            self.daily_start_energy = 0
    
    def _get_latest_energy_from_db(self, query_api):
        """Lấy giá trị energy gần nhất từ InfluxDB"""
        try:
            flux = f"""
from(bucket: "{INFLUX_BUCKET}")
  |> range(start: -30d)
  |> filter(fn: (r) => r["_measurement"] == "data")
  |> filter(fn: (r) => r["_field"] == "energy")
  |> last()
"""
            tables = query_api.query(flux, org=INFLUX_ORG)
            for table in tables:
                for record in table.records:
                    return float(record.get_value())
            return 0
        except Exception as e:
            logger.error(f"Lỗi lấy energy gần nhất: {e}")
            return 0
    
    def _get_month_start_energy_from_db(self, query_api):
        """Lấy giá trị energy tại đầu tháng từ InfluxDB"""
        try:
            now = datetime.now(TIMEZONE_GMT7)
            # Tính ngày đầu tháng
            month_start = now.replace(day=MONTH_START_DAY, hour=0, minute=0, second=0, microsecond=0)
            
            # Nếu hôm nay là ngày đầu tháng, lấy tháng trước
            if now.day < MONTH_START_DAY:
                if month_start.month == 1:
                    month_start = month_start.replace(year=month_start.year-1, month=12)
                else:
                    month_start = month_start.replace(month=month_start.month-1)
            
            # Query energy gần nhất tại thời điểm đầu tháng
            flux = f"""
from(bucket: "{INFLUX_BUCKET}")
  |> range(start: {month_start.strftime('%Y-%m-%dT%H:%M:%SZ')}, stop: {(month_start + timedelta(days=1)).strftime('%Y-%m-%dT%H:%M:%SZ')})
  |> filter(fn: (r) => r["_measurement"] == "data")
  |> filter(fn: (r) => r["_field"] == "energy")
  |> first()
"""
            tables = query_api.query(flux, org=INFLUX_ORG)
            for table in tables:
                for record in table.records:
                    return float(record.get_value())
            
            # Nếu không tìm thấy dữ liệu tại đầu tháng, lấy energy gần nhất trước đó
            flux_fallback = f"""
from(bucket: "{INFLUX_BUCKET}")
  |> range(start: -60d, stop: {month_start.strftime('%Y-%m-%dT%H:%M:%SZ')})
  |> filter(fn: (r) => r["_measurement"] == "data")
  |> filter(fn: (r) => r["_field"] == "energy")
  |> last()
"""
            tables = query_api.query(flux_fallback, org=INFLUX_ORG)
            for table in tables:
                for record in table.records:
                    return float(record.get_value())
            return 0
        except Exception as e:
            logger.error(f"Lỗi lấy energy đầu tháng: {e}")
            return 0
    
    def _get_daily_start_energy_from_db(self, query_api):
        """Lấy giá trị energy tại đầu ngày hôm nay từ InfluxDB"""
        try:
            now = datetime.now(TIMEZONE_GMT7)
            today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
            
            # Query energy gần nhất tại 00:00 hôm nay
            flux = f"""
from(bucket: "{INFLUX_BUCKET}")
  |> range(start: {today_start.strftime('%Y-%m-%dT%H:%M:%SZ')}, stop: {(today_start + timedelta(hours=1)).strftime('%Y-%m-%dT%H:%M:%SZ')})
  |> filter(fn: (r) => r["_measurement"] == "data")
  |> filter(fn: (r) => r["_field"] == "energy")
  |> first()
"""
            tables = query_api.query(flux, org=INFLUX_ORG)
            for table in tables:
                for record in table.records:
                    return float(record.get_value())
            
            # Nếu không tìm thấy dữ liệu tại 00:00, lấy energy gần nhất trước đó
            flux_fallback = f"""
from(bucket: "{INFLUX_BUCKET}")
  |> range(start: -7d, stop: {today_start.strftime('%Y-%m-%dT%H:%M:%SZ')})
  |> filter(fn: (r) => r["_measurement"] == "data")
  |> filter(fn: (r) => r["_field"] == "energy")
  |> last()
"""
            tables = query_api.query(flux_fallback, org=INFLUX_ORG)
            for table in tables:
                for record in table.records:
                    return float(record.get_value())
            return 0
        except Exception as e:
            logger.error(f"Lỗi lấy energy đầu ngày: {e}")
            return 0
    
    def _apply_fallback_logic(self):
        """Áp dụng logic fallback khi không tìm thấy dữ liệu baseline"""
        # Nếu không có dữ liệu energy gần nhất, không thể làm gì
        if self.last_energy_reading == 0:
            logger.warning("Không có dữ liệu energy, sử dụng giá trị mặc định 0")
            return
        
        # Nếu không tìm thấy baseline đầu tháng, sử dụng energy hiện tại
        if self.monthly_start_energy == 0:
            self.monthly_start_energy = self.last_energy_reading
            logger.warning(f"Không tìm thấy baseline đầu tháng, sử dụng energy hiện tại: {self.monthly_start_energy} kWh")
        
        # Nếu không tìm thấy baseline đầu ngày, sử dụng energy hiện tại
        if self.daily_start_energy == 0:
            self.daily_start_energy = self.last_energy_reading
            logger.warning(f"Không tìm thấy baseline đầu ngày, sử dụng energy hiện tại: {self.daily_start_energy} kWh")
        
        # Kiểm tra tính hợp lý: baseline không được lớn hơn energy hiện tại
        if self.monthly_start_energy > self.last_energy_reading:
            logger.warning(f"Baseline đầu tháng ({self.monthly_start_energy}) > energy hiện tại ({self.last_energy_reading}), điều chỉnh về {self.last_energy_reading}")
            self.monthly_start_energy = self.last_energy_reading
        
        if self.daily_start_energy > self.last_energy_reading:
            logger.warning(f"Baseline đầu ngày ({self.daily_start_energy}) > energy hiện tại ({self.last_energy_reading}), điều chỉnh về {self.last_energy_reading}")
            self.daily_start_energy = self.last_energy_reading
    
    def _detect_pzem_reset(self, new_energy):
        """Phát hiện PZEM004T bị reset counter"""
        if self.last_energy_reading is None:
            return  # Lần đầu tiên nhận dữ liệu
        
        # Phát hiện energy giảm đột ngột (có thể do PZEM reset)
        if new_energy < self.last_energy_reading * 0.5:
            logger.critical("🚨 PZEM RESET DETECTED!")
            logger.critical(f"   Energy giảm từ {self.last_energy_reading} kWh xuống {new_energy} kWh")
            logger.critical(f"   Điều này có thể do:")
            logger.critical(f"   - PZEM004T bị reset/mất điện")
            logger.critical(f"   - ESP8266 reboot")
            logger.critical(f"   - Lỗi sensor hoặc nhiễu điện từ")
            logger.critical(f"   ⚠️  CẢNH BÁO: Dữ liệu tiêu thụ điện sẽ bị sai!")
            logger.critical(f"   💡 KHUYẾN NGHỊ: Reset baseline thủ công hoặc kiểm tra phần cứng")
            
            # Có thể thêm logic gửi email/webhook alert ở đây
            self._trigger_pzem_reset_alert(self.last_energy_reading, new_energy)
    
    def _trigger_pzem_reset_alert(self, old_energy, new_energy):
        """Kích hoạt cảnh báo khi phát hiện PZEM reset"""
        try:
            # Ghi vào InfluxDB để tracking
            alert_data = {
                "alert_type": "pzem_reset",
                "old_energy": old_energy,
                "new_energy": new_energy,
                "energy_drop_ratio": new_energy / old_energy if old_energy > 0 else 0,
                "severity": "critical"
            }
            
            write_influx(
                self.influx_client,
                "alerts",
                alert_data,
                {"alert_type": "pzem_reset"}
            )
            
            logger.info("Đã ghi alert vào InfluxDB measurement 'alerts'")
            
            # TODO: Thêm logic gửi email/webhook/Telegram notification
            # self._send_notification(alert_data)
            
        except Exception as e:
            logger.error(f"Lỗi khi ghi alert: {e}")
    
    def _validate_sensor_data(self, voltage, current, power, energy, frequency, pf):
        """Validate dữ liệu từ sensor PZEM004T"""
        
        # Kiểm tra giá trị âm (không hợp lệ cho điện năng)
        if energy < 0:
            logger.warning(f"Energy âm: {energy} kWh - không hợp lệ")
            return False
        
        # Kiểm tra voltage trong khoảng hợp lý (180V - 250V cho điện dân dụng VN)
        if voltage < 0 or voltage > 300:
            logger.warning(f"Voltage bất thường: {voltage}V")
            if voltage < 0:
                return False  # Voltage âm là không thể
        
        # Kiểm tra current âm
        if current < 0:
            logger.warning(f"Current âm: {current}A - không hợp lệ")
            return False
        
        # Kiểm tra power âm (có thể âm khi có năng lượng ngược, nhưng cảnh báo)
        if power < 0:
            logger.warning(f"Power âm: {power}W - có thể do năng lượng ngược")
        
        # Kiểm tra power spike bất thường (> 10kW)
        if power > 10000:
            logger.warning(f"Power spike cao: {power}W - kiểm tra thiết bị")
        
        # Kiểm tra frequency (49-51 Hz cho lưới điện VN)
        if frequency > 0 and (frequency < 45 or frequency > 55):
            logger.warning(f"Frequency bất thường: {frequency}Hz")
        
        # Kiểm tra power factor (0-1)
        if pf < 0 or pf > 1:
            logger.warning(f"Power factor không hợp lệ: {pf}")
            return False
        
        # Kiểm tra tính nhất quán: P = V × I × PF (cho AC)
        if voltage > 0 and current > 0 and pf > 0:
            calculated_power = voltage * current * pf
            power_diff_ratio = abs(power - calculated_power) / max(power, calculated_power, 1)
            
            if power_diff_ratio > 0.2:  # Chênh lệch > 20%
                logger.warning(f"Dữ liệu không nhất quán: P={power}W, V×I×PF={calculated_power:.1f}W (chênh {power_diff_ratio*100:.1f}%)")
        
        # Kiểm tra energy tăng quá nhanh (> 1kWh trong 1 message)
        if self.last_energy_reading is not None:
            energy_increase = energy - self.last_energy_reading
            if energy_increase > 1:  # Tăng > 1kWh trong 1 message (5s)
                logger.warning(f"Energy tăng quá nhanh: +{energy_increase:.3f} kWh trong 1 message")
                logger.warning("Có thể do lỗi sensor hoặc nhiễu điện từ")
        
        return True  # Dữ liệu hợp lệ
    
    def _health_check(self):
        """Kiểm tra sức khỏe hệ thống"""
        issues = []
        
        # Kiểm tra dữ liệu gần nhất
        if self.last_data_time is not None:
            time_since_last_data = (datetime.now(TIMEZONE_GMT7) - self.last_data_time).total_seconds()
            if time_since_last_data > 300:  # 5 phút không có dữ liệu
                issues.append(f"Không nhận dữ liệu trong {time_since_last_data/60:.1f} phút")
        
        # Kiểm tra InfluxDB
        try:
            self.influx_client.ping()
            self.influx_healthy = True
        except:
            self.influx_healthy = False
            issues.append("InfluxDB không khả dụng")
        
        # Kiểm tra baseline hợp lý
        if self.last_energy_reading is not None:
            if self.daily_start_energy and self.daily_start_energy > self.last_energy_reading:
                issues.append("Daily baseline > current energy")
            if self.monthly_start_energy and self.monthly_start_energy > self.last_energy_reading:
                issues.append("Monthly baseline > current energy")
        
        # Log health status
        if issues:
            logger.warning(f"🏥 Health Check Issues: {', '.join(issues)}")
            
            # Ghi health alert vào InfluxDB (nếu có thể)
            if self.influx_healthy:
                try:
                    health_data = {
                        "issues_count": len(issues),
                        "last_data_age_seconds": time_since_last_data if self.last_data_time else -1,
                        "influx_healthy": 1 if self.influx_healthy else 0,
                        "mqtt_connected": 1 if self.mqtt_connected else 0
                    }
                    
                    write_influx(
                        self.influx_client,
                        "health",
                        health_data,
                        {"status": "issues" if issues else "healthy"}
                    )
                except Exception as e:
                    logger.error(f"Lỗi ghi health data: {e}")
        else:
            logger.info("🏥 Health Check: All systems healthy")
        
        return len(issues) == 0

    def _midnight_job(self):
        try:
            self._reset_daily_energy()
            now = datetime.now(TIMEZONE_GMT7)
            if now.day == MONTH_START_DAY:
                self._reset_monthly_energy()
        except Exception as e:
            logger.error(f"Lỗi midnight job: {e}")
    
    def _reset_monthly_energy(self):
        """Reset energy baseline đầu tháng"""
        try:
            if self.last_energy_reading is not None:
                # Cập nhật baseline cho tháng mới
                self.monthly_start_energy = self.last_energy_reading
                logger.info(f"Reset monthly energy baseline: {self.monthly_start_energy} kWh")
        except Exception as e:
            logger.error(f"Lỗi reset monthly energy: {e}")
    
    def _reset_daily_energy(self):
        """Reset energy baseline đầu ngày"""
        try:
            if self.last_energy_reading is not None:
                # Cập nhật baseline cho ngày mới
                self.daily_start_energy = self.last_energy_reading
                logger.info(f"Reset daily energy baseline: {self.daily_start_energy} kWh")
        except Exception as e:
            logger.error(f"Lỗi reset daily energy: {e}")
    
    def process_mqtt_message(self, client, userdata, message):
        """Xử lý tin nhắn MQTT từ ESP8266"""
        try:
            topic = message.topic
            payload = message.payload.decode('utf-8')
            logger.info(f"Nhận dữ liệu từ {topic}: {payload}")
            
            # Parse JSON data từ ESP8266
            data = json.loads(payload)
            
            # Kiểm tra nếu có dữ liệu từ PZEM004T
            if all(key in data for key in ['voltage', 'current', 'power', 'energy', 'frequency', 'pf']):
                self._process_pzem_data(data)
            else:
                logger.warning(f"Dữ liệu không đầy đủ: {data}")
                
        except json.JSONDecodeError as e:
            logger.error(f"Lỗi parse JSON: {e}")
        except Exception as e:
            logger.error(f"Lỗi xử lý MQTT message: {e}")
    
    def _process_pzem_data(self, data: Dict):
        """Xử lý dữ liệu từ PZEM004T"""
        try:
            # Lấy timestamp hiện tại theo GMT+7
            timestamp = datetime.now(TIMEZONE_GMT7)
            
            # Extract dữ liệu cơ bản
            voltage = float(data.get('voltage', 0))
            current = float(data.get('current', 0))
            power = float(data.get('power', 0))
            energy = float(data.get('energy', 0))
            frequency = float(data.get('frequency', 0))
            pf = float(data.get('pf', 0))
            
            # Validation dữ liệu
            if not self._validate_sensor_data(voltage, current, power, energy, frequency, pf):
                logger.warning("Dữ liệu sensor không hợp lệ, bỏ qua message này")
                return
            
            # Phát hiện PZEM reset trước khi cập nhật
            self._detect_pzem_reset(energy)
            
            # Cập nhật energy reading và health status
            self.last_energy_reading = energy
            self.last_data_time = timestamp
            
            # Tính toán điện tiêu thụ
            daily_consumption = max(0, energy - self.daily_start_energy) if self.daily_start_energy is not None else 0
            monthly_consumption = max(0, energy - self.monthly_start_energy) if self.monthly_start_energy is not None else 0
            
            # Tính giá tiền theo bậc thang tích lũy trong tháng
            # - Giá điện hàng ngày được tính từ tổng tiêu thụ tích lũy từ đầu tháng
            # - Giá điện tháng là tổng giá của toàn bộ điện tiêu thụ trong tháng
            monthly_cost = calc_electricity_cost(monthly_consumption)
            # Tính giá điện hôm qua (tích lũy từ đầu tháng đến hết ngày hôm qua)
            yesterday_consumption = max(0, monthly_consumption - daily_consumption)
            yesterday_cost = calc_electricity_cost(yesterday_consumption)
            # Giá điện hôm nay = Tổng giá tích lũy - Tổng giá đến hết hôm qua
            daily_cost_value = monthly_cost['total'] - yesterday_cost['total']
            daily_cost = {
                'total': daily_cost_value,
                'kWh': daily_consumption,
                'subtotal': (monthly_cost['subtotal'] - yesterday_cost['subtotal']),
                'vat': (monthly_cost['vat'] - yesterday_cost['vat'])
            }
            
            # Ghi measurement 'data': dữ liệu realtime đầy đủ cho Grafana
            write_influx(
                self.influx_client,
                "data",
                {
                    # Thông số realtime từ PZEM
                    "voltage": voltage,
                    "current": current,
                    "power": power,
                    "energy": energy,
                    "frequency": frequency,
                    "power_factor": pf,
                    # Điện tiêu thụ hiện tại
                    "daily_kwh": daily_consumption,
                    "monthly_kwh": monthly_consumption,
                    # Tiền điện hiện tại
                    "daily_cost": daily_cost['total'],
                    "monthly_cost": monthly_cost['total']
                },
                {}
            )
            
            # Chỉ ghi dữ liệu realtime, không ghi snapshot
            
            logger.info(f"Đã xử lý dữ liệu: Power={power}W, Energy={energy}kWh, Daily={daily_consumption:.3f}kWh, Monthly={monthly_consumption:.3f}kWh")
            
        except Exception as e:
            logger.error(f"Lỗi xử lý dữ liệu PZEM: {e}")
    
    
    def get_consumption_summary(self) -> Dict:
        """Lấy tổng quan điện tiêu thụ"""
        try:
            # Tính toán hiện tại
            daily_consumption = max(0, self.last_energy_reading - self.daily_start_energy) if self.daily_start_energy is not None else 0
            monthly_consumption = max(0, self.last_energy_reading - self.monthly_start_energy) if self.monthly_start_energy is not None else 0
            
            # Tính giá tiền theo bậc thang tích lũy trong tháng
            monthly_cost = calc_electricity_cost(monthly_consumption)
            # Tính giá điện hôm qua (tích lũy từ đầu tháng đến hết ngày hôm qua)
            yesterday_consumption = max(0, monthly_consumption - daily_consumption)
            yesterday_cost = calc_electricity_cost(yesterday_consumption)
            # Giá điện hôm nay = Tổng giá tích lũy - Tổng giá đến hết hôm qua
            daily_cost_value = monthly_cost['total'] - yesterday_cost['total']
            daily_cost = {
                'total': daily_cost_value,
                'kWh': daily_consumption,
                'subtotal': (monthly_cost['subtotal'] - yesterday_cost['subtotal']),
                'vat': (monthly_cost['vat'] - yesterday_cost['vat'])
            }
            
            return {
                "timestamp": datetime.now(TIMEZONE_GMT7).isoformat(),
                "daily": {
                    "kwh": daily_consumption,
                    "cost": daily_cost['total']
                },
                "monthly_current": {
                    "kwh": monthly_consumption,
                    "cost": monthly_cost['total']
                }
            }
            
        except Exception as e:
            logger.error(f"Lỗi lấy consumption summary: {e}")
            return {}
    
    def run(self):
        """Chạy processor chính"""
        try:
            # Khởi tạo MQTT client
            mqtt_client = init_mqtt(self.process_mqtt_message)
            logger.info("Đã kết nối MQTT thành công")
            
            # Chạy MQTT loop trong background
            mqtt_client.loop_start()
            
            logger.info("ElectricityProcessor đang chạy...")
            
            # Main loop
            while True:
                # Chạy scheduled tasks
                schedule.run_pending()
                
                # Log trạng thái và health check mỗi 5 phút
                if int(time.time()) % 300 == 0:
                    summary = self.get_consumption_summary()
                    if summary:
                        logger.info(f"Trạng thái: Daily={summary['daily']['kwh']:.3f}kWh, Monthly={summary['monthly_current']['kwh']:.3f}kWh")
                    
                    # Health check
                    self._health_check()
                
                time.sleep(1)
                
        except KeyboardInterrupt:
            logger.info("Đang dừng ElectricityProcessor...")
            mqtt_client.loop_stop()
            mqtt_client.disconnect()
        except Exception as e:
            logger.error(f"Lỗi trong main loop: {e}")
            raise

if __name__ == "__main__":
    processor = ElectricityProcessor()
    processor.run()
