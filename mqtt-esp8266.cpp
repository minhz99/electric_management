#include <ESP8266WiFi.h>
#include <PubSubClient.h>
#include <SoftwareSerial.h>
#include <PZEM004Tv30.h>
#include <ArduinoJson.h>

// ===== Hằng số mạng (Sửa tại đây) =====
const char* ssid = "YOUR_WIFI_SSID";
const char* password = "YOUR_WIFI_PASSWORD";

// ===== Hằng số MQTT (Sửa tại đây) =====
// Đặt IP local của máy tính đang chạy Python Server
const char* mqtt_server = "192.168.1.xxx";
const int   mqtt_port   = 1883;
const char* mqtt_pub_topic  = "testtopic/pzem004t";    // Khớp với Config trên server

WiFiClient espClient;
PubSubClient client(espClient);

// ===== Firmware Setup PZEM =====
// D7 = GPIO13 = RX, D6 = GPIO12 = TX
SoftwareSerial pzemSWSerial(13, 12);
PZEM004Tv30 pzem(pzemSWSerial);

void setup_wifi() {
  WiFi.begin(ssid, password);
  Serial.print("Connecting WiFi");
  while (WiFi.status() != WL_CONNECTED) {
    delay(500);
    Serial.print(".");
  }
  Serial.println("\n[+] WiFi Connected!");
  Serial.print("[+] IP: ");
  Serial.println(WiFi.localIP());
}

void reconnect() {
  while (!client.connected()) {
    Serial.print("Connecting to local Broker...");
    if (client.connect("ESP8266_HomeEnergy")) {
      Serial.println(" [OK]");
    } else {
      Serial.print(" [Fail] rc=");
      Serial.print(client.state());
      Serial.println(" -> Retrying in 5s");
      delay(5000);
    }
  }
}

void setup() {
  Serial.begin(115200);
  setup_wifi();
  
  client.setServer(mqtt_server, mqtt_port);
  // Đoạn lệnh setCallback bị xoá đi do phiên bản nhẹ này chỉ chuyên Push, ko cần nhận lệnh reset cồng kềnh vì Database lo.
}

void loop() {
  if (!client.connected()) reconnect();
  client.loop();

  // Đọc PZEM V3
  float voltage = pzem.voltage();
  float current = pzem.current();
  float power   = pzem.power();
  float energy  = pzem.energy();
  float freq    = pzem.frequency();
  float pf      = pzem.pf();

  // Khởi tạo bộ nhớ Json siêu nhỏ, tự dọn rác
  StaticJsonDocument<128> doc;
  
  doc["voltage"]   = isnan(voltage) ? 0 : voltage;
  doc["current"]   = isnan(current) ? 0 : current;
  doc["power"]     = isnan(power)   ? 0 : power;
  doc["energy"]    = isnan(energy)  ? 0 : energy;
  doc["frequency"] = isnan(freq)    ? 0 : freq;
  doc["pf"]        = isnan(pf)      ? 0 : pf;

  char payload[128];
  serializeJson(doc, payload);

  // In Serial debug & Publish
  Serial.println(payload);
  client.publish(mqtt_pub_topic, payload);

  // Đẩy 5 giây/lần
  delay(5000); 
}