"""
Simulate MQTT power meter data for debugging.
Gửi payload JSON tương tự ESP8266 + PZEM004T.

Trường dữ liệu:
- voltage (V), current (A), power (W), energy (kWh), frequency (Hz), pf

Ví dụ chạy:
  python simulator.py --broker 127.0.0.1 --topic testtopic/pzem004t --interval 5
  python simulator.py --send-reset --cmd-topic testtopic/pzem004t/cmd
"""

import argparse
import json
import math
import random
import sys
import time
from datetime import datetime

import paho.mqtt.client as mqtt
from typing import Optional


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="MQTT Power Data Simulator")
    parser.add_argument("--broker", default="localhost", help="MQTT broker host")
    parser.add_argument("--port", type=int, default=1883, help="MQTT broker port")
    parser.add_argument(
        "--topic", default="testtopic/pzem004t", help="MQTT topic để publish dữ liệu"
    )
    parser.add_argument(
        "--cmd-topic",
        default="testtopic/pzem004t/cmd",
        help="MQTT topic để gửi lệnh reset",
    )
    parser.add_argument(
        "--interval", type=float, default=5.0, help="Chu kỳ gửi dữ liệu (giây)"
    )
    parser.add_argument(
        "--start-energy", type=float, default=0.0, help="Giá trị kWh bắt đầu"
    )
    parser.add_argument(
        "--power", type=float, default=350.0, help="Công suất danh định trung bình (W)"
    )
    parser.add_argument(
        "--variance", type=float, default=150.0, help="Độ dao động công suất (W)"
    )
    parser.add_argument(
        "--pf", type=float, default=0.92, help="Hệ số công suất trung bình"
    )
    parser.add_argument(
        "--voltage", type=float, default=227.0, help="Điện áp danh định (V)"
    )
    parser.add_argument(
        "--freq", type=float, default=50.0, help="Tần số danh định (Hz)"
    )
    parser.add_argument(
        "--send-reset",
        action="store_true",
        help="Gửi lệnh reset energy lên cmd-topic rồi thoát",
    )
    parser.add_argument(
        "--username", default=None, help="MQTT username (optional)"
    )
    parser.add_argument(
        "--password", default=None, help="MQTT password (optional)"
    )
    return parser


def connect_mqtt(host: str, port: int, username: Optional[str], password: Optional[str]) -> mqtt.Client:
    client = mqtt.Client()
    if username:
        client.username_pw_set(username, password)
    client.connect(host, port, 60)
    return client


def publish_reset(client: mqtt.Client, cmd_topic: str) -> None:
    payload = {"reset": True}
    client.publish(cmd_topic, json.dumps(payload))
    print(f"[MQTT] Sent reset to {cmd_topic}: {payload}")


def simulate_loop(
    client: mqtt.Client,
    topic: str,
    interval_s: float,
    start_energy_kwh: float,
    base_power_w: float,
    variance_w: float,
    voltage_v: float,
    frequency_hz: float,
    pf_avg: float,
):
    """Giả lập tải thay đổi theo thời gian, năng lượng tăng dần."""

    energy_kwh = start_energy_kwh
    t0 = time.time()
    last_ts = t0

    while True:
        now = time.time()
        dt = max(0.0, now - last_ts)
        last_ts = now

        # Mô phỏng profile phụ tải ngày: sóng sin theo thời gian + nhiễu ngẫu nhiên
        day_phase = (now % (24 * 3600)) / (24 * 3600) * 2 * math.pi
        diurnal = 0.25 * math.sin(day_phase - math.pi / 2) + 0.75  # [0.5 .. 1.0]
        noise = random.uniform(-1.0, 1.0)
        power_w = max(0.0, base_power_w * diurnal + variance_w * 0.3 * noise)

        # Hệ số công suất dao động nhẹ quanh pf_avg
        pf = max(0.5, min(1.0, random.gauss(pf_avg, 0.02)))

        # Dòng điện I = P / (U * pf)
        current_a = 0.0
        if voltage_v * pf > 0:
            current_a = power_w / (voltage_v * pf)

        # Tăng dần energy: kWh += (W * s) / 3,600,000
        energy_kwh += (power_w * dt) / 3_600_000.0

        payload = {
            "voltage": round(voltage_v + random.uniform(-2.0, 2.0), 2),
            "current": round(current_a, 3),
            "power": round(power_w, 1),
            "energy": round(energy_kwh, 3),
            "frequency": round(frequency_hz + random.uniform(-0.05, 0.05), 2),
            "pf": round(pf, 3),
        }

        client.publish(topic, json.dumps(payload))
        print(f"[MQTT] Published to {topic}: {payload}")

        # Chờ đến lần gửi tiếp theo, nhưng tích lũy dt chính xác
        sleep_left = interval_s - (time.time() - now)
        if sleep_left > 0:
            time.sleep(sleep_left)


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    try:
        client = connect_mqtt(args.broker, args.port, args.username, args.password)
    except Exception as e:
        print(f"Không thể kết nối MQTT: {e}")
        return 1

    if args.send_reset:
        publish_reset(client, args.cmd_topic)
        client.disconnect()
        return 0

    print(
        "Bắt đầu giả lập: broker=%s port=%d topic=%s interval=%.1fs start_energy=%.3f kWh"
        % (args.broker, args.port, args.topic, args.interval, args.start_energy)
    )

    try:
        simulate_loop(
            client=client,
            topic=args.topic,
            interval_s=args.interval,
            start_energy_kwh=args.start_energy,
            base_power_w=args.power,
            variance_w=args.variance,
            voltage_v=args.voltage,
            frequency_hz=args.freq,
            pf_avg=args.pf,
        )
    except KeyboardInterrupt:
        print("Đã dừng giả lập (Ctrl+C)")
    finally:
        try:
            client.disconnect()
        except Exception:
            pass

    return 0


if __name__ == "__main__":
    sys.exit(main())


