# main.py
"""
File chính để chạy hệ thống quản lý điện năng.
Serve giao diện web tĩnh (frontend/) và chạy MQTT broker + processor.
"""

import logging
import logging.handlers
import sys
import asyncio
import threading
import time
import os
from processor import ElectricityProcessor
from broker import start_mqtt_broker

# ── Logging ──────────────────────────────────────────────────────────────────
# RotatingFileHandler: tối đa 5 MB × 3 file backup → không phình log không giới hạn
_log_handler_file = logging.handlers.RotatingFileHandler(
    'electricity_management.log', maxBytes=5 * 1024 * 1024, backupCount=3, encoding='utf-8'
)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)-8s %(message)s',
    handlers=[_log_handler_file, logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# ── MQTT Broker (amqtt, chạy trên thread riêng) ───────────────────────────────
async def _amqtt_loop():
    broker = await start_mqtt_broker()
    if broker:
        while True:
            await asyncio.sleep(60)

def _run_broker():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(_amqtt_loop())

# ── Static file server ────────────────────────────────────────────────────────
def start_web_server(port: int = 5535):
    """Serve thư mục frontend/ bằng Python HTTP server thuần."""
    import http.server
    import socketserver

    root = os.path.join(os.path.dirname(__file__), 'frontend')
    if not os.path.isdir(root):
        logger.error(f"Không tìm thấy thư mục frontend/: {root}")
        return

    class Handler(http.server.SimpleHTTPRequestHandler):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, directory=root, **kwargs)

        def end_headers(self):
            # Cache-Control: cho phép browser / CDN cache file tĩnh; không cache HTML
            path = self.path.split('?')[0]
            if path.endswith(('.js', '.css', '.png', '.ico', '.woff2')):
                self.send_header('Cache-Control', 'public, max-age=86400')
            else:
                self.send_header('Cache-Control', 'no-cache')
            # CORS: cho phép Cloudflare tunnel và các host khác
            self.send_header('Access-Control-Allow-Origin', '*')
            super().end_headers()

        def log_message(self, *_):
            pass  # Tắt HTTP access log (console sạch hơn)

    class ReuseServer(socketserver.TCPServer):
        allow_reuse_address = True

    def _serve():
        try:
            with ReuseServer(('0.0.0.0', port), Handler) as httpd:
                logger.info(f"🌐 Giao diện web: http://localhost:{port}  (LAN: http://<IP>:{port})")
                httpd.serve_forever()
        except Exception as e:
            logger.error(f"❌ Lỗi web server: {e}")

    threading.Thread(target=_serve, daemon=True, name='web-server').start()

# ── Entry point ───────────────────────────────────────────────────────────────
def main():
    logger.info("=== Khởi động Electric Management ===")

    # 1. MQTT Broker
    threading.Thread(target=_run_broker, daemon=True, name='mqtt-broker').start()

    # 2. Web server
    start_web_server()

    # Chờ broker sẵn sàng
    time.sleep(2)

    # 3. Processor (blocking — chiếm luồng chính)
    try:
        processor = ElectricityProcessor()
        processor.run()
    except KeyboardInterrupt:
        logger.info("Đã dừng bởi người dùng.")
    except Exception as e:
        logger.error(f"Lỗi nghiêm trọng: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
