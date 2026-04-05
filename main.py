# main.py
"""
File chính để chạy hệ thống quản lý điện năng
"""

import logging
import sys
import asyncio
import threading
import time
import subprocess
import os
from processor import ElectricityProcessor
from broker import start_mqtt_broker

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('electricity_management.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

async def amqtt_loop():
    broker = await start_mqtt_broker()
    if broker:
        while True:
            await asyncio.sleep(1)
    else:
        logger.error("Broker failed format.")

def run_broker_thread():
    """Khởi chạy AMQTT Broker trên luồng riêng"""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(amqtt_loop())

def start_web_frontend():
    """Tự động chạy giao diện Web trên cổng 5535"""
    frontend_path = os.path.join(os.path.dirname(__file__), 'kinetic-precision')
    try:
        logger.info("🚀 Đang khởi động giao diện Web trên cổng 5535...")
        # Bắt đầu npm run dev trong background và redirect output để tránh làm rối log main
        subprocess.Popen(["npm", "run", "dev"], cwd=frontend_path, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
    except Exception as e:
        logger.error(f"❌ Không thể chạy giao diện Web: {e}")

def main():
    """Hàm main chạy ứng dụng"""
    try:
        logger.info("=== Khởi động hệ thống quản lý điện năng All-In-One ===")
        
        # 1. Khởi chạy Local MQTT Broker
        broker_thread = threading.Thread(target=run_broker_thread, daemon=True)
        broker_thread.start()
        
        # 2. Khởi chạy Giao diện Web
        start_web_frontend()
        
        # Chờ Broker và Web khởi động sơ bộ
        time.sleep(2)
        
        # 3. Khởi tạo và chạy Processor (sẽ tự động connect vào 127.0.0.1:1883)
        processor = ElectricityProcessor()
        processor.run()
        
    except KeyboardInterrupt:
        logger.info("Hệ thống đã được dừng bởi người dùng")
    except Exception as e:
        logger.error(f"Lỗi nghiêm trọng: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
