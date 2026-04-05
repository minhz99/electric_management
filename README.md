# Hệ thống Quản lý Điện năng ESP8266 + PZEM004T (All-in-One)

Hệ thống giám sát và quản lý điện năng sử dụng ESP8266, cảm biến PZEM004T. Thiết kế mới nhất gộp thẳng **MQTT Broker** vào máy chủ Python và sử dụng **Google Firebase** (Realtime DB) thay vì cài đặt InfluxDB phức tạp. Giao diện trực quan tích hợp React Web.

## Tính năng

- ✅ **Built-in MQTT Broker:** Không cần cài đặt cồng kềnh (Mosquitto/EMQX) trên PC. Cắm ESP8266 là chạy thẳng vào máy luôn.
- ✅ Tự động khôi phục dữ liệu năng lượng tiêu thụ (chống mất điện/tắt server) với hệ thống lưu biến `state` cục bộ.
- ✅ Hiển thị qua giao diện React hiện đại có Realtime Dashboard.
- ✅ Tính tiền điện theo bậc thang (bao gồm VAT 8%).
- ✅ Push Realtime thẳng lên nền tảng đám mây miễn phí Firebase.

## Kiến trúc hệ thống
1. **Thiết bị:** ESP8266 kết nối cảm biến PZEM004T.
2. **Server Local (Python):** Mở port TCP `1883` để đón MQTT từ ESP, tự động đóng gói dữ liệu và tính tiền rồi dội thẳng lên Firebase RTDB.
3. **App (React):** Theo dõi Live kết quả được đồng bộ từ đám mây (không cần setup ngrok port forwarding).

## Cài đặt

1. **Clone repository:**
```bash
git clone <repository-url>
cd electric_management
```

2. **Cài đặt dependencies Python:**
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

3. **Thiết lập Firebase trên Cloud:**
Làm theo file tài liệu `walkthrough.md` để lấy thông tin Database của bạn (gồm `serviceAccountKey.json` cho Python Server và khóa Web cho React/Vite). Cập nhật các hằng số bằng cách copy file example:
```bash
cp env.example .env
```

4. **Nạp Code ESP8266:**
Mở `mqtt-esp8266.cpp` và thay IP Broker thành địa chỉ IPv4 nội bộ (LAN) của máy tính bạn đang chạy Server Python.

## Chạy hệ thống

1. **Bật máy chủ Data Pipeline:**
Gõ lệnh này (đảm bảo môi trường Venv):
```bash
python main.py
```
*Giao diện của bạn sẽ báo `Bult-in MQTT Broker đang lắng nghe ở cổng TCP 1883...`*

2. **Bật Panel theo dõi:**
Bạn mở một tag/terminal khác, đi vào thư mục web:
```bash
cd kinetic-precision
npm install
npm run dev
```

Server sẽ tự hoạt động vĩnh viễn (Realtime). Nếu mất điện, Python sẽ đọc lệnh từ Node `state` trên firebase để phục hồi chỉ số đầu ngày.