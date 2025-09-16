# processor.py
"""
Module xử lý dữ liệu MQTT từ ESP8266 PZEM004T và gửi vào InfluxDB
Tính toán điện tiêu thụ hàng ngày/tháng và dự đoán
"""

import json
import logging
from datetime import datetime
import schedule
import time
from typing import Dict

from config import init_influx, write_influx, init_mqtt, INFLUX_BUCKET, INFLUX_ORG, DAILY_RESET_TIME, MONTH_START_DAY
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
        
        # Khởi tạo giá trị ban đầu
        self._initialize_energy_baseline()
        
        # Schedule job lúc DAILY_RESET_TIME mỗi ngày. Trong job sẽ reset daily
        # và nếu là ngày đầu tháng thì reset monthly.
        schedule.every().day.at(DAILY_RESET_TIME).do(self._midnight_job)
        
        logger.info("Đã khởi tạo ElectricityProcessor")
    
    def _initialize_energy_baseline(self):
        """Khởi tạo baseline energy cho tháng/ngày hiện tại"""
        try:
            # Lấy giá trị energy gần nhất từ InfluxDB (v2 - Flux)
            query_api = self.influx_client.query_api()
            flux = f"""
from(bucket: "{INFLUX_BUCKET}")
  |> range(start: -30d)
  |> filter(fn: (r) => r["_measurement"] == "data")
  |> filter(fn: (r) => r["_field"] == "energy")
  |> last()
"""
            tables = query_api.query(flux, org=INFLUX_ORG)
            val = None
            for table in tables:
                for record in table.records:
                    val = record.get_value()
            if val is not None:
                self.last_energy_reading = float(val)
                logger.info(f"Khởi tạo energy baseline (v2): {self.last_energy_reading} kWh")
            else:
                self.last_energy_reading = 0
                logger.info("Không tìm thấy dữ liệu energy (v2), khởi tạo = 0")
            
            # Thiết lập baseline cho tháng và ngày
            self.monthly_start_energy = self.last_energy_reading
            self.daily_start_energy = self.last_energy_reading
            
        except Exception as e:
            logger.error(f"Lỗi khởi tạo energy baseline: {e}")
            self.last_energy_reading = 0
            self.monthly_start_energy = 0
            self.daily_start_energy = 0

    def _midnight_job(self):
        try:
            self._reset_daily_energy()
            now = datetime.now()
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
            # Lấy timestamp hiện tại
            timestamp = datetime.utcnow()
            
            # Extract dữ liệu cơ bản
            voltage = float(data.get('voltage', 0))
            current = float(data.get('current', 0))
            power = float(data.get('power', 0))
            energy = float(data.get('energy', 0))
            frequency = float(data.get('frequency', 0))
            pf = float(data.get('pf', 0))
            
            # Cập nhật energy reading
            self.last_energy_reading = energy
            
            # Tính toán điện tiêu thụ
            daily_consumption = max(0, energy - self.daily_start_energy) if self.daily_start_energy is not None else 0
            monthly_consumption = max(0, energy - self.monthly_start_energy) if self.monthly_start_energy is not None else 0
            
            # Tính giá tiền
            daily_cost = calc_electricity_cost(daily_consumption)
            monthly_cost = calc_electricity_cost(monthly_consumption)
            
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
            
            # Tính giá tiền
            daily_cost = calc_electricity_cost(daily_consumption)
            monthly_cost = calc_electricity_cost(monthly_consumption)
            
            return {
                "timestamp": datetime.now().isoformat(),
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
                
                # Log trạng thái mỗi 5 phút
                if int(time.time()) % 300 == 0:
                    summary = self.get_consumption_summary()
                    if summary:
                        logger.info(f"Trạng thái: Daily={summary['daily']['kwh']:.3f}kWh, Monthly={summary['monthly_current']['kwh']:.3f}kWh")
                
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
