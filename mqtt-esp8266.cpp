#include <ESP8266WiFi.h>
#include <PubSubClient.h>
#include <SoftwareSerial.h>
#include <PZEM004Tv30.h>
#include <ArduinoJson.h>

// ===== WiFi Config =====
const char* ssid = "YOUR_WIFI_SSID";
const char* password = "YOUR_WIFI_PASSWORD";

// ===== MQTT Config =====
const char* mqtt_server = "192.168.100.51";   // EMQX broker IP
const int   mqtt_port   = 1883;
const char* mqtt_pub_topic  = "testtopic/pzem004t";      // Topic gửi dữ liệu
const char* mqtt_sub_topic  = "testtopic/pzem004t/cmd";  // Topic nhận lệnh

WiFiClient espClient;
PubSubClient client(espClient);

// ===== PZEM Config =====
// D7 = GPIO13 = RX, D6 = GPIO12 = TX
SoftwareSerial pzemSWSerial(13, 12);
PZEM004Tv30 pzem(pzemSWSerial);

void setup_wifi() {
  WiFi.begin(ssid, password);
  Serial.print("Dang ket noi WiFi");
  while (WiFi.status() != WL_CONNECTED) {
    delay(500);
    Serial.print(".");
  }
  Serial.println("\nWiFi da ket noi!");
  Serial.print("IP: ");
  Serial.println(WiFi.localIP());
}

// ===== Xử lý khi nhận tin nhắn MQTT =====
void callback(char* topic, byte* payload, unsigned int length) {
  Serial.print("Tin nhan MQTT nhan duoc [");
  Serial.print(topic);
  Serial.print("]: ");

  String msg;
  for (unsigned int i = 0; i < length; i++) {
    msg += (char)payload[i];
  }
  Serial.println(msg);

  // Parse JSON
  StaticJsonDocument<128> doc;
  DeserializationError error = deserializeJson(doc, msg);
  if (!error) {
    if (doc.containsKey("reset") && doc["reset"] == true) {
      Serial.println(">>> RESET ENERGY <<<");
      pzem.resetEnergy();
      client.publish(mqtt_pub_topic, "{\"status\":\"energy reset\"}");
    }
  }
}

void reconnect() {
  while (!client.connected()) {
    Serial.print("Dang ket noi MQTT...");
    if (client.connect("ESP8266_PZEM")) {
      Serial.println(" Da ket noi!");
      client.subscribe(mqtt_sub_topic);  // Đăng ký topic lệnh
    } else {
      Serial.print(" Loi, rc=");
      Serial.print(client.state());
      Serial.println(" -> Thu lai sau 5s");
      delay(5000);
    }
  }
}

void setup() {
  Serial.begin(115200);
  setup_wifi();
  client.setServer(mqtt_server, mqtt_port);
  client.setCallback(callback);
}

void loop() {
  if (!client.connected()) reconnect();
  client.loop();

  // ===== Đọc dữ liệu từ PZEM =====
  float voltage = pzem.voltage();
  float current = pzem.current();
  float power   = pzem.power();
  float energy  = pzem.energy();
  float freq    = pzem.frequency();
  float pf      = pzem.pf();

  // ===== JSON Payload =====
  StaticJsonDocument<192> doc;
  doc["voltage"]   = isnan(voltage) ? 0 : voltage;
  doc["current"]   = isnan(current) ? 0 : current;
  doc["power"]     = isnan(power)   ? 0 : power;
  doc["energy"]    = isnan(energy)  ? 0 : energy;
  doc["frequency"] = isnan(freq)    ? 0 : freq;
  doc["pf"]        = isnan(pf)      ? 0 : pf;

  char payload[192];
  serializeJson(doc, payload);

  // Debug Serial
  Serial.println(payload);

  // Gửi MQTT
  client.publish(mqtt_pub_topic, payload);

  delay(5000); // gửi mỗi 5 giây
}