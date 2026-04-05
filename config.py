# config.py
"""
Cấu hình & hàm tiện ích cho MQTT và InfluxDB
"""

import paho.mqtt.client as mqtt
import firebase_admin
from firebase_admin import credentials, db
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

# PZEM: tổng kWh trên module không reset; offset = giá trị ghi khi bắt đầu theo dõi (ví dụ 3160).
# State trên Firebase lưu kWh đã trừ offset (tọa độ "adjusted").
PZEM_ENERGY_OFFSET_KWH = float(os.getenv("PZEM_ENERGY_OFFSET_KWH", "0"))

# MQTT Configuration
MQTT_BROKER = os.getenv("MQTT_BROKER", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", 1883))
MQTT_USER = os.getenv("MQTT_USER", "")
MQTT_PASS = os.getenv("MQTT_PASS", "")
MQTT_TOPICS = os.getenv("MQTT_TOPICS", "testtopic/pzem004t").split(",") # danh sách topic cần subscribe

# Firebase Configuration
FIREBASE_DATABASE_URL = os.getenv("FIREBASE_DATABASE_URL")
FIREBASE_CREDENTIALS_PATH = os.getenv("FIREBASE_CREDENTIALS_PATH", "serviceAccountKey.json")

# ==========================
# FIREBASE FUNCTIONS
# ==========================

def init_firebase():
    """Khởi tạo Firebase Admin SDK"""
    if not FIREBASE_DATABASE_URL:
        print("CẢNH BÁO: Chưa cấu hình FIREBASE_DATABASE_URL trong file .env")
        return None
        
    try:
        if not firebase_admin._apps:
            cred = credentials.Certificate(FIREBASE_CREDENTIALS_PATH)
            firebase_admin.initialize_app(cred, {
                'databaseURL': FIREBASE_DATABASE_URL
            })
        print("✅ Kết nối Firebase thành công!")
        return db.reference('/')
    except Exception as e:
        print(f"❌ Lỗi kết nối Firebase: {e}")
        return None

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