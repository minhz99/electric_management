# main.py
"""
File chính để chạy hệ thống quản lý điện năng.
Chạy MQTT broker + processor.
"""

import logging
import logging.handlers
import sys
import asyncio
import threading
import time
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

# ── Main ─────────────────────────────────────────────────────────────────────
def main():
    logger.info("=== Bắt đầu khởi động hệ thống ===")
    
    # 1. Chạy Broker
    threading.Thread(target=_run_broker, daemon=True, name='mqtt-broker').start()

    logger.info("🌐 Dashboard live reload dùng Vite tại http://localhost:5535")
    logger.info("   Chạy `cd kinetic-precision && npm run dev` ở terminal khác để mở UI.")

    # Đợi sơ bộ
    time.sleep(2)

    # 2. Chạy Processor
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
