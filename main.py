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
    """Tự động chạy giao diện Web trên cổng 5535 bằng Python HTTP Server (không chạy nền quá trình npm)"""
    import http.server
    import socketserver
    
    frontend_path = os.path.join(os.path.dirname(__file__), 'kinetic-precision')
    dist_path = os.path.join(frontend_path, 'dist')
    
    if not os.path.exists(dist_path):
        logger.info("⚙️ Đang tiến hành đóng gói (build) ứng dụng React trong nền (chỉ chạy lần đầu)...")
        try:
            subprocess.run(["npm", "install"], cwd=frontend_path, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
            subprocess.run(["npm", "run", "build"], cwd=frontend_path, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
            logger.info("✅ Build giao diện web thành công!")
        except Exception as e:
            logger.error(f"❌ Lỗi khi đóng gói React: {e}")
            return

    PORT = 5535

    class Handler(http.server.SimpleHTTPRequestHandler):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, directory=dist_path, **kwargs)
            
        def log_message(self, format, *args):
            pass # Tắt log HTTP của server tĩnh để console sạch sẽ hơn

    class CustomTCPServer(socketserver.TCPServer):
        allow_reuse_address = True

    def run_server():
        try:
            with CustomTCPServer(("", PORT), Handler) as httpd:
                logger.info(f"🚀 Khởi động trang quản lý tại: http://localhost:{PORT}")
                httpd.serve_forever()
        except Exception as e:
            logger.error(f"❌ Lỗi port 5535: {e}")

    server_thread = threading.Thread(target=run_server, daemon=True)
    server_thread.start()

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
