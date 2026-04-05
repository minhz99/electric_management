"""
Local MQTT Broker sử dụng amqtt
Giúp nhận dữ liệu từ ESP8266 trực tiếp mà không cần cài Mosquitto.
"""
import logging
from amqtt.broker import Broker
import asyncio

logger = logging.getLogger(__name__)

broker_config = {
    'listeners': {
        'default': {
            'type': 'tcp',
            'bind': '0.0.0.0:1883',
            'max_connections': 50000,
        }
    },
    'sys_interval': 10,
    'auth': {
        'allow-anonymous': True,
        'plugins': ['auth_anonymous']
    },
    'topic-check': {
        'enabled': False
    }
}

async def start_mqtt_broker():
    """Khởi động MQTT Server"""
    try:
        broker = Broker(broker_config)
        await broker.start()
        logger.info("🟢 Bult-in MQTT Broker đang trực chờ ở cổng TCP 1883...")
        return broker
    except Exception as e:
        logger.error(f"Lỗi khi khởi động MQTT Broker: {e}")
        return None
