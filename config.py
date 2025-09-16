# config.py
"""
Cấu hình & hàm tiện ích cho MQTT và InfluxDB
"""

import paho.mqtt.client as mqtt
from influxdb_client import InfluxDBClient as InfluxDBClientV2, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

# ==========================
# CONFIGURATION
# ==========================

# Electricity Pricing Configuration
PRICE_TIERS = [
    (52, 1984),              # Bậc 1: 0-52 kWh
    (52, 2050),              # Bậc 2: 53-105 kWh
    (103, 2380),             # Bậc 3: 106-208 kWh
    (103, 2998),             # Bậc 4: 209-311 kWh
    (103, 3350),             # Bậc 5: 312-414 kWh
    (float("inf"), 3460)     # Bậc 6: >414 kWh
]
VAT_RATE = 0.08  # Thuế VAT (8%)

# Time Configuration
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

def init_influx():
    if not INFLUX_TOKEN:
        raise RuntimeError("INFLUX_TOKEN chưa được cấu hình cho InfluxDB v2")
    url = f"http://{INFLUX_HOST}:{INFLUX_PORT}"
    client = InfluxDBClientV2(url=url, token=INFLUX_TOKEN, org=INFLUX_ORG)
    return client


def write_influx(client, measurement, fields, tags=None):
    """
    Ghi dữ liệu vào InfluxDB
    Args:
        client: đối tượng InfluxDBClient
        measurement (str): tên bảng (measurement)
        fields (dict): dữ liệu cần ghi { "power": 100, "voltage": 220 }
        tags (dict): tags cho dữ liệu (VD: {"device": "esp32"})
    """
    write_api = client.write_api(write_options=SYNCHRONOUS)
    p = Point(measurement)
    for k, v in (tags or {}).items():
        p = p.tag(k, str(v))
    for k, v in fields.items():
        p = p.field(k, v)
    p = p.time(datetime.utcnow())
    write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=p)

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