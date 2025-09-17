# config.py
"""
Cấu hình & hàm tiện ích cho MQTT và InfluxDB
"""

import paho.mqtt.client as mqtt
from influxdb_client import InfluxDBClient as InfluxDBClientV2, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import datetime, timezone, timedelta
import os
from dotenv import load_dotenv

load_dotenv()

# ==========================
# CONFIGURATION
# ==========================

# Electricity Pricing Configuration
PRICE_TIERS = [
    (50, 1984),              # Bậc 1: 0-50 kWh
    (50, 2050),              # Bậc 2: 51-100 kWh
    (100, 2380),             # Bậc 3: 101-200 kWh
    (100, 2998),             # Bậc 4: 201-300 kWh
    (100, 3350),             # Bậc 5: 301-400 kWh
    (float("inf"), 3460)     # Bậc 6: >400 kWh
]
VAT_RATE = 0.08  # Thuế VAT (8%)

# Time Configuration
TIMEZONE_GMT7 = timezone(timedelta(hours=7))  # Múi giờ GMT+7 (Việt Nam)
DAILY_RESET_TIME = "00:00"  # Thời điểm reset đầu ngày theo định dạng HH:MM (24h)
MONTH_START_DAY = 1         # Ngày bắt đầu của tháng (thường là 1)

# MQTT Configuration
MQTT_BROKER = os.getenv("MQTT_BROKER")
MQTT_PORT = int(os.getenv("MQTT_PORT", 1883))
MQTT_USER = os.getenv("MQTT_USER", "")
MQTT_PASS = os.getenv("MQTT_PASS", "")
MQTT_TOPICS = os.getenv("MQTT_TOPICS").split(",") # danh sách topic cần subscribe

# InfluxDB Configuration (v2 only)
INFLUX_HOST = os.getenv("INFLUX_HOST")
INFLUX_PORT = int(os.getenv("INFLUX_PORT", 8086))
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN")
INFLUX_ORG = os.getenv("INFLUX_ORG")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET")

# ==========================
# INFLUXDB FUNCTIONS
# ==========================

def init_influx(max_retries=3):
    """Khởi tạo InfluxDB client với retry logic"""
    if not INFLUX_TOKEN:
        raise RuntimeError("INFLUX_TOKEN chưa được cấu hình cho InfluxDB v2")
    
    url = f"http://{INFLUX_HOST}:{INFLUX_PORT}"
    
    for attempt in range(max_retries):
        try:
            client = InfluxDBClientV2(url=url, token=INFLUX_TOKEN, org=INFLUX_ORG)
            # Test connection
            client.ping()
            return client
        except Exception as e:
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt  # Exponential backoff: 1s, 2s, 4s
                print(f"InfluxDB connection failed (attempt {attempt + 1}/{max_retries}): {e}")
                print(f"Retrying in {wait_time} seconds...")
                import time
                time.sleep(wait_time)
            else:
                raise RuntimeError(f"Failed to connect to InfluxDB after {max_retries} attempts: {e}")


def write_influx(client, measurement, fields, tags=None, max_retries=2):
    """
    Ghi dữ liệu vào InfluxDB với retry logic
    Args:
        client: đối tượng InfluxDBClient
        measurement (str): tên bảng (measurement)
        fields (dict): dữ liệu cần ghi { "power": 100, "voltage": 220 }
        tags (dict): tags cho dữ liệu (VD: {"device": "esp32"})
        max_retries (int): số lần retry tối đa
    """
    for attempt in range(max_retries + 1):
        try:
            write_api = client.write_api(write_options=SYNCHRONOUS)
            p = Point(measurement)
            for k, v in (tags or {}).items():
                p = p.tag(k, str(v))
            for k, v in fields.items():
                p = p.field(k, v)
            p = p.time(datetime.now(TIMEZONE_GMT7))
            write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=p)
            return  # Thành công, thoát khỏi hàm
        except Exception as e:
            if attempt < max_retries:
                wait_time = 0.5 * (2 ** attempt)  # Exponential backoff: 0.5s, 1s
                print(f"InfluxDB write failed (attempt {attempt + 1}/{max_retries + 1}): {e}")
                print(f"Retrying in {wait_time} seconds...")
                import time
                time.sleep(wait_time)
            else:
                # Log error nhưng không raise để không crash hệ thống
                print(f"Failed to write to InfluxDB after {max_retries + 1} attempts: {e}")
                print("⚠️  Data will be lost, but system continues running")

# ==========================
# MQTT FUNCTIONS
# ==========================

def init_mqtt(on_message_callback):
    """
    Khởi tạo MQTT client
    Args:
        on_message_callback: hàm callback khi nhận tin nhắn
    """
    client = mqtt.Client()
    # Chỉ set username/password nếu có cấu hình
    if MQTT_USER:
        client.username_pw_set(MQTT_USER, MQTT_PASS)
    client.on_connect = lambda c, u, f, rc: c.subscribe([(topic, 0) for topic in MQTT_TOPICS])
    client.on_message = on_message_callback
    client.connect(MQTT_BROKER, MQTT_PORT, 60)
    return client