# Electric Management System (ESP8266 + PZEM-004T)

An "All-in-One" electricity monitoring and management system designed for real-time tracking of energy consumption. This project integrates hardware (ESP8266 & PZEM sensor), a Python-based backend with a built-in MQTT broker, and a modern React dashboard, with data persistence powered by Google Firebase.

## 🚀 Key Features

- **Built-in MQTT Broker:** Uses a native Python-based MQTT broker (AMQTT), eliminating the need for external services like Mosquitto.
- **Real-time Synchronization:** Leverages Google Firebase Realtime Database for instantaneous data updates across all connected clients.
- **Robust State Recovery:** Automatically saves and recovers consumption metrics (Daily/Monthly kWh) to prevent data loss during power outages or server restarts.
- **Tiered Pricing Calculation:** Automatically calculates electricity costs based on the standard Vietnamese (EVN) tiered pricing model (including 8% VAT).
- **Modern Dashboard:** A high-performance React dashboard (Vite + TypeScript) for visualizing voltage, current, power, and cost analytics.
- **Zero Port Forwarding:** Firebase integration allows remote monitoring without complex network configurations like Ngrok or DDNS.

## 🏗 System Architecture

1.  **Hardware Level:** The ESP8266 microchip reads electrical data from the PZEM-004T v3.0 sensor and publishes JSON payloads via MQTT.
2.  **Infrastructure Level:** A Python server hosts a local MQTT broker on port `1883`.
3.  **Processing Level:** The `ElectricityProcessor` (Python) subscribes to MQTT topics, processes raw data, calculates costs, and syncs state to Firebase.
4.  **UI Level:** The React application (`kinetic-precision`) fetches real-time data directly from Firebase to display live charts and metrics.

## 🛠 Tech Stack

-   **Firmware:** C++ (Arduino Framework), ESP8266WiFi, PubSubClient, ArduinoJson.
-   **Backend:** Python 3.x, AMQTT (Broker), Paho-MQTT (Client), Firebase Admin SDK.
-   **Frontend:** React, Vite, TypeScript, TailwindCSS.
-   **Database:** Google Firebase Realtime Database.

## 📦 Installation & Setup

### 1. Prerequisites
- Python 3.9+
- Node.js & npm
- A Firebase Project (Realtime Database enabled)

### 2. Backend Setup
1.  Clone the repository:
    ```bash
    git clone https://github.com/your-repo/electric_management.git
    cd electric_management
    ```
2.  Create and activate a virtual environment:
    ```bash
    python3 -m venv .venv
    source .venv/bin/activate  # MacOS/Linux
    # .venv\Scripts\activate   # Windows
    ```
3.  Install dependencies:
    ```bash
    pip install -r requirements.txt
    ```
4.  Configure environment variables:
    ```bash
    cp env.example .env
    ```
    Edit `.env` and provide your Firebase `FIREBASE_DATABASE_URL` and `FIREBASE_CREDENTIALS_PATH` (path to your `serviceAccountKey.json`).

### 3. Frontend Setup
1.  Navigate to the web directory:
    ```bash
    cd kinetic-precision
    npm install
    ```
2.  Configure Firebase for the web:
    Create a `.env` file in `kinetic-precision` (referencing `.env.example`) and add your Firebase Web Config keys.

### 4. Firmware Configuration
1.  Open `mqtt-esp8266.cpp` in your Arduino IDE or VS Code/PlatformIO.
2.  Update `ssid`, `password`, and `mqtt_server` (your computer's local IP address).
3.  Upload the code to your ESP8266.

## ⚡ Running the System

Build dashboard React trước:

```bash
cd kinetic-precision
npm run build
cd ..
```

Sau đó khởi động toàn bộ hệ thống (Broker, Processor, và web server tĩnh):

```bash
python main.py
```

-   The **MQTT Broker** will start on `tcp://0.0.0.0:1883`.
-   The **Data Processor** will begin listening for sensor data.
-   The **Web Server** will serve `kinetic-precision/dist` on port `5535` (fallback to `frontend/` only if no production build is found).

## 💰 Electricity Pricing (EVN Tiers)

The system is configured to calculate costs based on the following tiers (VND/kWh):

| Tier | Range (kWh) | Price (VND) |
| :--- | :--- | :--- |
| Tier 1 | 0 - 50 | 1,984 |
| Tier 2 | 51 - 100 | 2,050 |
| Tier 3 | 101 - 200 | 2,380 |
| Tier 4 | 201 - 300 | 2,998 |
| Tier 5 | 301 - 400 | 3,350 |
| Tier 6 | > 400 | 3,460 |

*Note: A 0.08 (8%) VAT rate is applied to the subtotal.*

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
