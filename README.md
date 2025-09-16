# Hệ thống Quản lý Điện năng ESP8266 + PZEM004T

Hệ thống giám sát và quản lý điện năng sử dụng ESP8266, cảm biến PZEM004T, MQTT và InfluxDB.

## Tính năng

- ✅ Nhận dữ liệu điện năng từ ESP8266 qua MQTT
- ✅ Tính toán điện tiêu thụ hàng ngày và hàng tháng
- ✅ Tính giá tiền theo biểu giá điện bậc thang Việt Nam (bao gồm VAT)
- ✅ Tự động reset số liệu đầu tháng và đầu ngày
- ✅ Lưu trữ dữ liệu vào InfluxDB
- ✅ Logging chi tiết

## Dữ liệu được thu thập

### Dữ liệu thô từ PZEM004T:
- Điện áp (V)
- Dòng điện (A) 
- Công suất (W)
- Điện năng (kWh)
- Tần số (Hz)
- Hệ số công suất

### Dữ liệu được tính toán (đã lưu InfluxDB):
- **data**: Dữ liệu realtime (6 thông số + tiêu thụ và tiền điện hiện tại)
- **daily**: Snapshot ngày đã kết thúc (kWh và tiền điện theo ngày)  
- **monthly**: Snapshot tháng đã kết thúc (kWh và tiền điện theo tháng)

## Cài đặt

1. **Clone repository:**
```bash
git clone <repository-url>
cd electric_management
```

2. **Cài đặt dependencies:**
```bash
pip install -r requirements.txt
```

3. **Cấu hình môi trường (InfluxDB v2 bắt buộc):**
```bash
cp env.example .env
# Chỉnh sửa file .env với thông tin của bạn
```

4. **Cài đặt InfluxDB v2:**
```bash
# Ubuntu/Debian
sudo apt update
sudo apt install influxdb
sudo systemctl start influxdb
sudo systemctl enable influxdb

# macOS
brew install influxdb
brew services start influxdb
```

## Cấu hình

### File `.env`:
Sao chép từ `env.example` và điền thông tin của bạn:
```bash
cp env.example .env
```

Các biến quan trọng cần cấu hình:
- `MQTT_BROKER`: IP địa chỉ MQTT broker
- `INFLUX_TOKEN`: Token xác thực InfluxDB v2 (lấy từ InfluxDB UI)
- `INFLUX_ORG`: Tên organization trong InfluxDB v2
- Các biến khác có thể giữ mặc định

### ESP8266 Configuration:
File `mqtt-esp8266.cpp` đã được cấu hình để:
- Kết nối WiFi
- Gửi dữ liệu PZEM004T qua MQTT mỗi 5 giây
- Nhận lệnh reset energy qua MQTT

## Chạy hệ thống

```bash
INFLUX_HOST=192.168.100.51 INFLUX_PORT=8086 \
INFLUX_TOKEN=... INFLUX_ORG=... INFLUX_BUCKET=electricity \
MQTT_BROKER=192.168.100.51 MQTT_TOPICS=testtopic/pzem004t \
python main.py
```

## Cấu trúc Database (InfluxDB)

### Measurement: `data` (Dữ liệu realtime)
- **Fields**: 
  - `voltage`, `current`, `power`, `energy`, `frequency`, `power_factor` (từ PZEM)
  - `daily_kwh`, `monthly_kwh` (điện tiêu thụ hiện tại)
  - `daily_cost`, `monthly_cost` (tiền điện hiện tại)
- **Tags**: `device=ESP8266_PZEM`, `location=main`, `period=current`

### Measurement: `daily` (Snapshot theo ngày)  
- **Fields**: `energy_day_kwh`, `cost_total`
- **Tags**: `period=daily`, `date=YYYY-MM-DD`, `year`, `month`, `device`

### Measurement: `monthly` (Snapshot theo tháng)
- **Fields**: `energy_month_kwh`, `cost_total`
- **Tags**: `period=monthly`, `month=YYYY-MM`, `year`, `device`

### Measurement: `system_events`
- **Fields**: `event`, `energy_baseline`
- **Tags**: `type=reset`

## API Usage

### Lấy tổng quan tiêu thụ:
```python
from processor import ElectricityProcessor

processor = ElectricityProcessor()
summary = processor.get_consumption_summary()
print(summary)
```

### Kết quả example:
```json
{
  "timestamp": "2024-01-15T10:30:00",
  "daily": {
    "kwh": 2.5,
    "cost": 5250
  },
  "monthly_current": {
    "kwh": 45.2,
    "cost": 95430
  }
}
```

## Biểu giá điện (Việt Nam - 2025)

| Bậc | Mức tiêu thụ (kWh) | Đơn giá (đồng/kWh) |
|-----|--------------------|--------------------|
| 1   | 0 - 52             | 1,984              |
| 2   | 53 - 105           | 2,050              |
| 3   | 106 - 208          | 2,380              |
| 4   | 209 - 311          | 2,998              |
| 5   | 312 - 414          | 3,350              |
| 6   | > 414              | 3,460              |

*Đã bao gồm VAT 8%*

## Cấu hình Grafana

### 1. Cài đặt Grafana
```bash
# Ubuntu/Debian
sudo apt update
sudo apt install grafana
sudo systemctl start grafana-server
sudo systemctl enable grafana-server

# macOS
brew install grafana
brew services start grafana

# Access: http://localhost:3000 (admin/admin)
```

### 2. Thêm Data Source InfluxDB
- **URL**: `http://localhost:8086` (hoặc IP InfluxDB server)
- **Database**: `electricity` (InfluxDB v1) hoặc **Bucket**: `electricity` (InfluxDB v2)
- **User/Token**: Điền thông tin xác thực

### 3. Tạo Dashboard

#### Panel: Thông số điện realtime
```flux
from(bucket: "electricity")
  |> range(start: -1h)
  |> filter(fn: (r) => r["_measurement"] == "data")
  |> filter(fn: (r) => r["_field"] == "voltage" or r["_field"] == "current" or r["_field"] == "power")
  |> aggregateWindow(every: 30s, fn: mean)
```

#### Panel: Tiêu thụ điện hôm nay
```flux
from(bucket: "electricity")
  |> range(start: today())
  |> filter(fn: (r) => r["_measurement"] == "data")
  |> filter(fn: (r) => r["_field"] == "daily_kwh" or r["_field"] == "daily_cost")
  |> aggregateWindow(every: 5m, fn: last)
```

#### Panel: Tiêu thụ điện tháng này
```flux
from(bucket: "electricity")
  |> range(start: 2025-01-01T00:00:00Z) // Thay đổi theo tháng hiện tại
  |> filter(fn: (r) => r["_measurement"] == "data")
  |> filter(fn: (r) => r["_field"] == "monthly_kwh" or r["_field"] == "monthly_cost")
  |> aggregateWindow(every: 1h, fn: last)
```

#### Panel: Lịch sử theo tháng
```flux
from(bucket: "electricity")
  |> range(start: -1y)
  |> filter(fn: (r) => r["_measurement"] == "monthly")
  |> filter(fn: (r) => r["_field"] == "energy_month_kwh" or r["_field"] == "cost_total")
  |> sort(columns: ["_time"])
```

#### Panel: Lịch sử theo ngày (30 ngày gần nhất)
```flux
from(bucket: "electricity")
  |> range(start: -30d)
  |> filter(fn: (r) => r["_measurement"] == "daily")
  |> filter(fn: (r) => r["_field"] == "energy_day_kwh" or r["_field"] == "cost_total")
  |> sort(columns: ["_time"])
```

### 4. Các loại biểu đồ khuyến nghị

#### Realtime Dashboard:
- **Time Series**: Điện áp, dòng điện, công suất theo thời gian thực
- **Stat Panel**: Giá trị hiện tại (V, A, W, kWh)
- **Gauge**: Hệ số công suất (0-1)
- **Bar Chart**: Tiêu thụ điện hôm nay vs. hôm qua

#### Historical Dashboard:
- **Bar Chart**: So sánh tiêu thụ điện theo tháng (kWh và VNĐ)
- **Heatmap**: Mức tiêu thụ theo giờ trong ngày
- **Table**: Top 10 ngày tiêu thụ nhiều nhất
- **Pie Chart**: Phân bổ chi phí theo bậc giá điện

#### Trend Analysis:
- **Time Series**: Xu hướng tiêu thụ 30 ngày gần nhất
- **Progress Bar**: % hoàn thành mục tiêu tiết kiệm điện

### 5. Thiết lập cảnh báo (Alert)
- **Điều kiện**: Công suất > 2000W hoặc tiền điện tháng > 500,000 VNĐ
- **Thông báo**: Email, Slack, Telegram

## Logs

Hệ thống ghi log vào:
- Console output
- File: `electricity_management.log`

## Troubleshooting

1. **Lỗi kết nối MQTT:**
   - Kiểm tra IP broker trong `.env`
   - Đảm bảo ESP8266 và server cùng mạng

2. **Lỗi InfluxDB:**
   - Kiểm tra InfluxDB đã chạy: `systemctl status influxdb`
   - Kiểm tra cấu hình trong `.env`

3. **Dữ liệu không chính xác:**
   - Kiểm tra log để xem ESP8266 có gửi dữ liệu không
   - Reset energy trên ESP8266 nếu cần: gửi `{"reset": true}` tới topic `testtopic/pzem004t/cmd`
