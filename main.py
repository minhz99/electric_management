# main.py
"""
File chính để chạy hệ thống quản lý điện năng.
Serve dashboard web tĩnh và chạy MQTT broker + processor.
"""

import logging
import logging.handlers
import sys
import asyncio
import threading
import time
import os
import http.server
import socketserver
from processor import ElectricityProcessor
from broker import start_mqtt_broker

# ── Logging ──────────────────────────────────────────────────────────────────
_log_handler_file = logging.handlers.RotatingFileHandler(
    'electricity_management.log', maxBytes=5 * 1024 * 1024, backupCount=3, encoding='utf-8'
)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)-8s %(message)s',
    handlers=[_log_handler_file, logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# ── MQTT Broker ──────────────────────────────────────────────────────────────
async def _amqtt_loop():
    broker = await start_mqtt_broker()
    if broker:
        while True:
            await asyncio.sleep(60)

def _run_broker():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(_amqtt_loop())
    except Exception as e:
        logger.error(f"Lỗi Broker: {e}")

# ── Static web server ────────────────────────────────────────────────────────
def _resolve_web_root() -> str:
    project_root = os.path.dirname(os.path.abspath(__file__))
    dist_root = os.path.join(project_root, 'kinetic-precision', 'dist')
    legacy_root = os.path.join(project_root, 'frontend')

    if os.path.exists(dist_root):
        logger.info(f"🌐 Phục vụ dashboard React production từ: {dist_root}")
        return dist_root

    logger.warning(
        "⚠️ Không tìm thấy kinetic-precision/dist, fallback sang frontend cũ. "
        "Chạy `cd kinetic-precision && npm run build` để dùng dashboard React làm mặc định."
    )
    return legacy_root


def start_web_server(port: int = 5535):
    root = _resolve_web_root()
    
    if not os.path.exists(root):
        logger.error(f"❌ KHÔNG THẤY THƯ MỤC FRONTEND TẠI: {root}")
        # Tạo folder nếu chưa có để tránh crash
        os.makedirs(root, exist_ok=True)

    class CustomHandler(http.server.SimpleHTTPRequestHandler):
        def __init__(self, *args, **kwargs):
            # Ép buộc sử dụng thư mục frontend
            super().__init__(*args, directory=root, **kwargs)

        def end_headers(self):
            path = self.path.split('?')[0]
            if path.endswith(('.js', '.css', '.png', '.ico', '.woff2')):
                self.send_header('Cache-Control', 'public, max-age=86400')
            else:
                self.send_header('Cache-Control', 'no-cache')
            self.send_header('Access-Control-Allow-Origin', '*')
            super().end_headers()

        def log_message(self, format, *args):
            # Logger cho web server để dễ debug xem file có được load không
            logger.debug(f"HTTP: {self.address_string()} - {format%args}")

    def _serve():
        # Thử tắt các tiến trình cũ nếu port đang bị chiếm
        socketserver.TCPServer.allow_reuse_address = True
        try:
            with socketserver.TCPServer(("0.0.0.0", port), CustomHandler) as httpd:
                logger.info(f"🚀 WEB SERVER ĐANG CHẠY: http://localhost:{port}")
                logger.info(f"📂 Thư mục phục vụ: {root}")
                httpd.serve_forever()
        except OSError as e:
            if e.errno == 48: # Address already in use
                logger.error(f"❌ PORT {port} ĐÃ BỊ CHIẾM! Hãy tắt npm run dev hoặc các app khác đang dùng port này.")
            else:
                logger.error(f"❌ Lỗi khởi động Web Server: {e}")
        except Exception as e:
            logger.error(f"❌ Lỗi không xác định: {e}")

    t = threading.Thread(target=_serve, daemon=True, name='web-server')
    t.start()

# ── Main ─────────────────────────────────────────────────────────────────────
def main():
    logger.info("=== Bắt đầu khởi động hệ thống ===")
    
    # 1. Chạy Broker
    threading.Thread(target=_run_broker, daemon=True, name='mqtt-broker').start()

    # 2. Chạy Web Server
    start_web_server(5535)

    # Đợi sơ bộ
    time.sleep(2)

    # 3. Chạy Processor
    try:
        processor = ElectricityProcessor()
        processor.run()
    except KeyboardInterrupt:
        logger.info("Dừng chương trình...")
    except Exception as e:
        logger.error(f"Lỗi hệ thống: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
