#include <WiFi.h>
#include <esp_wifi.h> 
#include <esp_arduino_version.h>
#include <WiFiManager.h>
#include <ModbusMaster.h>
#include <FirebaseESP32.h> 
#include <esp_task_wdt.h> 
#include <esp_system.h> 
#include <esp_heap_caps.h> 
#include <driver/uart.h>
#include <soc/soc.h>             
#include <soc/rtc_cntl_reg.h>    
#include <atomic> 
#include <math.h>
#include <string.h>
#include <time.h>
#include <Preferences.h> 
#include <HTTPClient.h>
#include <HTTPUpdate.h>
#include <WiFiClientSecure.h>

// ================= MACRO LOGGING & TIMEOUT =================
#define DEBUG_MODE 1
#if DEBUG_MODE
  #define DLOG(x) Serial.println(x)
  #define DLOGF(...) Serial.printf(__VA_ARGS__)
#else
  #define DLOG(x)
  #define DLOGF(...)
#endif

#define MUTEX_TIMEOUT_MS 15 
#define CORE_HEARTBEAT_TIMEOUT_MS 45000 
#define FIREBASE_STUCK_TIMEOUT_MS 120000 

// ================= CẤU HÌNH PHẦN CỨNG & FIREBASE =================
#define DEFAULT_FIREBASE_HOST "sosen-inverter-default-rtdb.asia-southeast1.firebasedatabase.app" 
#define DEFAULT_FIREBASE_AUTH "LNAQyUsJYWrxWBTO7tdJc78r9jiGMrVopnOYQEOT"      
#define LED_PIN        2  
#define TRIGGER_PIN    0  
#define RX2_PIN        16 
#define TX2_PIN        17 

const char* NTP_SERVER_1 = "pool.ntp.org";
const char* NTP_SERVER_2 = "time.google.com";
const long  GMT_OFFSET_SEC = 7 * 3600;
const int   DAYLIGHT_OFFSET_SEC = 0;

ModbusMaster node;
FirebaseData fbdo; 
FirebaseData streamData; 
FirebaseAuth auth;
FirebaseConfig config;
Preferences preferences;

String global_fb_host = "";
String global_fb_auth = "";
String saved_wifi_ssid = "";
String saved_wifi_pass = "";

std::atomic<unsigned long> core0_heartbeat(0);
std::atomic<unsigned long> core1_heartbeat(0);
std::atomic<bool> isCommandPending(false);
std::atomic<bool> hasInitialModbusSuccess(false);
std::atomic<bool> isUpdatingOTA(false); 

std::atomic<uint32_t> stat_successRead(0);
std::atomic<uint32_t> stat_crcError(0);
std::atomic<uint32_t> stat_timeoutError(0);
std::atomic<uint32_t> stat_wifiReconnects(0);
std::atomic<uint32_t> stat_uartResets(0);
std::atomic<uint32_t> stat_firebaseRecoveries(0);
std::atomic<uint32_t> stat_historySnapshots(0);
std::atomic<unsigned long> stat_lastReadTime(0);
std::atomic<unsigned long> stat_lastUploadTime(0);
std::atomic<unsigned long> stat_lastFirebaseSuccessTime(0);
std::atomic<int> firebaseConsecutiveFails(0);
std::atomic<uint32_t> stat_successWrite(0);
std::atomic<uint32_t> stat_mutexTimeouts(0);
std::atomic<uint32_t> stat_commandDrops(0);
std::atomic<uint32_t> stat_modbusCycles(0);
std::atomic<uint32_t> stat_modbusCycleErrors(0);
std::atomic<uint32_t> stat_energyReadErrors(0);
std::atomic<unsigned long> stat_lastGridGoodTime(0);
std::atomic<unsigned long> stat_lastPvGoodTime(0);
std::atomic<unsigned long> stat_lastBatteryGoodTime(0);
std::atomic<unsigned long> stat_lastInverterTempGoodTime(0);

RTC_DATA_ATTR int bootCount = 0;
RTC_DATA_ATTR int resetReason = 0;

enum InverterState {
  INV_OFF = 0,
  INV_ON = 1
};

typedef struct {
  float pv_power, grid_power, grid_import_power, grid_export_power, grid_voltage, grid_frequency;
  float bat_voltage, bat_current, bat_power, inv_temp, bat_temp; 
  float load_power; 
  float load_power_raw;
  bool load_calculation_valid;
  int bat_soc;
  InverterState inv_switch_state;
  char bat_status[16];
  bool grid_state_known;
  bool grid_data_fresh;
  bool grid_power_fresh;
  bool grid_online;
  bool pv_data_fresh;
  bool battery_data_fresh;
  bool inverter_temp_fresh;
  bool inv_command_known;
  char grid_state[12];
  char grid_raw_state[12];
} LiveData_t;

typedef struct {
  float pv_today, grid_buy_today, grid_sell_today, bat_discharge_today, bat_charge_today, load_today;
  float pv_total, grid_buy_total, grid_sell_total, load_total, bat_discharge_total, bat_charge_total; 
} EnergyData_t;

SemaphoreHandle_t liveMutex; 
SemaphoreHandle_t energyMutex;
QueueHandle_t liveQueue;
QueueHandle_t energyQueue;
QueueHandle_t commandQueue;

LiveData_t currentLive;       
LiveData_t lastReadData;      
LiveData_t lastUploadedData;  
EnergyData_t currentEnergySnapshot{};
bool hasLastReadData = false;

uint32_t modbusPollCounter = 0;
unsigned long lastModbusCycleTime = 0;
unsigned long lastModbusPollStartTime = 0;

const unsigned long CYCLE_INTERVAL_LIVE = 4000;
const unsigned long LIVE_FORCE_UPLOAD_INTERVAL = 60000; 
const unsigned long HEARTBEAT_INTERVAL = 4000;          
const unsigned long WIFI_OFFLINE_RESTART_MS = 10UL * 60UL * 1000UL;
const unsigned long FIREBASE_STREAM_RETRY_INTERVAL = 15000;
const unsigned long HISTORY_SNAPSHOT_INTERVAL_MS = 300UL * 60UL * 1000UL; 

const uint8_t FIREBASE_UPDATE_RETRIES = 2;
const uint16_t UART_STREAM_TIMEOUT_MS = 200;
const uint16_t MODBUS_MIN_POLL_GAP_MS = 50; 
const uint32_t WATCHDOG_TIMEOUT_MS = 45000;
const uint32_t INVERTER_DATA_STALE_MS = 20000; 
const bool MODBUS_USE_HOLDING_REGISTERS = false; 

const float PV_POWER_SCALE_W = 10.0;
const float GRID_POWER_SCALE_W = 10.0; 
const bool INVERTER_GRID_IMPORT_IS_POSITIVE = false;

const float PV_POWER_CHANGE_THRESHOLD_W = 15.0;
const float GRID_POWER_CHANGE_THRESHOLD_W = 15.0;
const float BAT_POWER_CHANGE_THRESHOLD_W = 15.0; 
const float LOAD_POWER_CHANGE_THRESHOLD_W = 15.0; 
const float GRID_VOLTAGE_CHANGE_THRESHOLD_V = 5.0;
const float GRID_FREQUENCY_CHANGE_THRESHOLD_HZ = 0.2;

const float GRID_VOLTAGE_SCALE = 0.1;
const float GRID_FREQUENCY_SCALE = 0.01;

const uint8_t ENERGY_REGISTER_COUNT = 19;
const uint8_t COMMAND_QUEUE_LENGTH = 1;
const float MAX_DAILY_ENERGY_KWH = 1000.0;
const float MAX_LIFETIME_ENERGY_KWH = 10000000.0;

int consecutiveModbusErrors = 0;
const int MAX_MODBUS_ERRORS = 5; 

int gridDebounceCounter = 0;
char lastRawGridState[12] = "UNKNOWN";

unsigned long lastHistorySaveMillis = 0;
int lastMidnightSnapshotDay = -1;

unsigned long blinkTimer = 0;
bool isBlinking = false;

void triggerBlink() {
  digitalWrite(LED_PIN, HIGH);
  blinkTimer = millis();
  isBlinking = true;
}

void handleBlink() {
  if (isBlinking && (millis() - blinkTimer >= 20)) {
    digitalWrite(LED_PIN, LOW);
    isBlinking = false;
  }
}

void feedWatchdog() {
  esp_task_wdt_reset();
}

void hardwareClearUART(bool forceFlush = false) {
  node.clearResponseBuffer();
  if (forceFlush) {
    Serial2.flush();
    uart_flush_input(UART_NUM_2);
  }
}

void waitForModbusPollGap() {
  unsigned long now = millis();
  if (lastModbusPollStartTime != 0) {
    unsigned long elapsed = now - lastModbusPollStartTime;
    if (elapsed < MODBUS_MIN_POLL_GAP_MS) {
      feedWatchdog();
      vTaskDelay(pdMS_TO_TICKS(MODBUS_MIN_POLL_GAP_MS - elapsed));
    }
  }
  lastModbusPollStartTime = millis();
}

void delayWithWatchdog(uint32_t delayMs) {
  feedWatchdog();
  if (delayMs > 0) {
    vTaskDelay(pdMS_TO_TICKS(delayMs));
  } else {
    taskYIELD();
  }
  feedWatchdog();
}

float normalizeGridPowerSign(float rawGridPowerW) {
  return INVERTER_GRID_IMPORT_IS_POSITIVE ? -rawGridPowerW : rawGridPowerW;
}

uint8_t getModbusMaxAttempts() {
  return (consecutiveModbusErrors >= 2) ? 1 : 2;
}

uint8_t readInverterRegistersWithRetry(uint16_t address, uint16_t count, uint8_t maxAttempts) {
  uint8_t result;
  uint8_t retryCount = 0;
  do {
    hardwareClearUART(retryCount > 0);
    waitForModbusPollGap();
    if (MODBUS_USE_HOLDING_REGISTERS) {
      result = node.readHoldingRegisters(address, count);
    } else {
      result = node.readInputRegisters(address, count);
    }
    if (result == node.ku8MBSuccess) {
      stat_successRead++;
      return result;
    }
    retryCount++;
    if (result == node.ku8MBResponseTimedOut) stat_timeoutError++; else stat_crcError++;
    delayWithWatchdog(10);
  } while (retryCount < maxAttempts);
  return result;
}

uint8_t readHoldingRegistersWithRetry(uint16_t address, uint16_t count, uint8_t maxAttempts) {
  uint8_t result;
  uint8_t retryCount = 0;
  do {
    hardwareClearUART(retryCount > 0);
    waitForModbusPollGap();
    result = node.readHoldingRegisters(address, count);
    if (result == node.ku8MBSuccess) {
      stat_successRead++;
      return result;
    }
    retryCount++;
    if (result == node.ku8MBResponseTimedOut) stat_timeoutError++; else stat_crcError++;
    delayWithWatchdog(10);
  } while (retryCount < maxAttempts);
  return result;
}

uint8_t writeInverterSingleRegisterWithRetry(uint16_t address, uint16_t value, uint8_t maxAttempts) {
  uint8_t result;
  uint8_t retryCount = 0;
  do {
    hardwareClearUART(retryCount > 0);
    waitForModbusPollGap();
    result = node.writeSingleRegister(address, value);
    if (result == node.ku8MBSuccess) {
      stat_successWrite++;
      return result;
    }
    retryCount++;
    if (result == node.ku8MBResponseTimedOut) stat_timeoutError++; else stat_crcError++;
    delayWithWatchdog(10);
  } while (retryCount < maxAttempts);
  return result;
}

void setBatteryStatus(LiveData_t &data, const char *status) {
  memset(data.bat_status, 0, sizeof(data.bat_status));
  strncpy(data.bat_status, status, sizeof(data.bat_status) - 1);
}

LiveData_t getCurrentLiveSnapshot() {
  LiveData_t snapshot{};
  if (liveMutex != NULL && xSemaphoreTake(liveMutex, pdMS_TO_TICKS(MUTEX_TIMEOUT_MS)) == pdTRUE) {
    snapshot = currentLive;
    xSemaphoreGive(liveMutex);
  } else {
    stat_mutexTimeouts++;
  }
  return snapshot;
}

void commitCurrentLive(LiveData_t &data) {
  if (liveMutex != NULL && xSemaphoreTake(liveMutex, pdMS_TO_TICKS(MUTEX_TIMEOUT_MS)) == pdTRUE) {
    InverterState latestSwitchState = currentLive.inv_switch_state;
    bool latestCommandKnown = currentLive.inv_command_known;
    currentLive = data;
    currentLive.inv_switch_state = latestSwitchState;
    currentLive.inv_command_known = latestCommandKnown;
    data.inv_switch_state = latestSwitchState;
    data.inv_command_known = latestCommandKnown;
    xSemaphoreGive(liveMutex);
  } else {
    stat_mutexTimeouts++;
  }
}

void setCurrentInvSwitchState(InverterState state) {
  if (liveMutex != NULL && xSemaphoreTake(liveMutex, pdMS_TO_TICKS(MUTEX_TIMEOUT_MS)) == pdTRUE) {
    currentLive.inv_switch_state = state;
    currentLive.inv_command_known = true;
    xSemaphoreGive(liveMutex);
  } else {
    stat_mutexTimeouts++;
  }
}

void commitEnergySnapshot(const EnergyData_t &e) {
  if (energyMutex != NULL && xSemaphoreTake(energyMutex, pdMS_TO_TICKS(MUTEX_TIMEOUT_MS)) == pdTRUE) {
    currentEnergySnapshot = e;
    xSemaphoreGive(energyMutex);
  }
}

EnergyData_t getEnergySnapshot() {
  EnergyData_t temp{};
  if (energyMutex != NULL && xSemaphoreTake(energyMutex, pdMS_TO_TICKS(MUTEX_TIMEOUT_MS)) == pdTRUE) {
    temp = currentEnergySnapshot;
    xSemaphoreGive(energyMutex);
  }
  return temp;
}

void setGridPowerW(LiveData_t &data, float signedGridPowerW) {
  data.grid_power = signedGridPowerW;
  data.grid_import_power = (signedGridPowerW < 0.0f) ? -signedGridPowerW : 0.0f;
  data.grid_export_power = (signedGridPowerW > 0.0f) ? signedGridPowerW : 0.0f;
}

bool isValidInverterCommand(int cmd) { return cmd == INV_OFF || cmd == INV_ON; }

bool isValidEnergyData(const EnergyData_t &data) {
  if (!isfinite(data.pv_today) || !isfinite(data.grid_buy_today) || !isfinite(data.grid_sell_today) || !isfinite(data.load_today)) return false;
  if (!isfinite(data.bat_discharge_today) || !isfinite(data.bat_charge_today)) return false;
  if (!isfinite(data.pv_total) || !isfinite(data.grid_buy_total) || !isfinite(data.grid_sell_total) || !isfinite(data.load_total)) return false;
  if (data.pv_today < 0 || data.pv_today > MAX_DAILY_ENERGY_KWH) return false;
  if (data.pv_total < 0 || data.pv_total > MAX_LIFETIME_ENERGY_KWH) return false;
  return true;
}

bool isValidData(const LiveData_t &data) {
  if (!isfinite(data.pv_power) || !isfinite(data.grid_power)) return false;
  if (!isfinite(data.grid_voltage) || !isfinite(data.grid_frequency)) return false;
  if (!isfinite(data.bat_voltage) || !isfinite(data.bat_current) || !isfinite(data.inv_temp) || !isfinite(data.bat_temp)) return false;
  if (data.bat_voltage < 10.0f && data.pv_power <= 0.0f && data.grid_voltage <= 10.0f) return false; 
  if (data.pv_power < 0 || data.pv_power > 15000) return false;
  if (fabsf(data.grid_power) > 15000) return false;
  if (data.grid_voltage < 0 || data.grid_voltage > 300.0f) return false;
  if (data.grid_frequency < 0 || data.grid_frequency > 70.0f) return false;
  if (data.bat_soc < 0 || data.bat_soc > 100) return false;
  if (data.inv_temp < -20 || data.inv_temp > 120) return false;
  if (data.bat_temp < -40 || data.bat_temp > 120) return false;
  return true;
}

bool isDataChanged(const LiveData_t &current, const LiveData_t &last) {
  if (fabsf(current.pv_power - last.pv_power) >= PV_POWER_CHANGE_THRESHOLD_W) return true;
  if (fabsf(current.grid_power - last.grid_power) >= GRID_POWER_CHANGE_THRESHOLD_W) return true;
  if (fabsf(current.bat_power - last.bat_power) >= BAT_POWER_CHANGE_THRESHOLD_W) return true;
  if (fabsf(current.load_power - last.load_power) >= LOAD_POWER_CHANGE_THRESHOLD_W) return true;
  if (fabsf(current.grid_voltage - last.grid_voltage) > GRID_VOLTAGE_CHANGE_THRESHOLD_V) return true;
  if (fabsf(current.grid_frequency - last.grid_frequency) > GRID_FREQUENCY_CHANGE_THRESHOLD_HZ) return true;
  if (fabsf(current.bat_voltage - last.bat_voltage) > 0.5f) return true;
  if (fabsf(current.bat_current - last.bat_current) > 1.0f) return true;
  if (fabsf(current.inv_temp - last.inv_temp) > 0.5f) return true;
  if (fabsf(current.bat_temp - last.bat_temp) > 0.5f) return true;
  if (current.load_calculation_valid != last.load_calculation_valid) return true;
  if (current.bat_soc != last.bat_soc) return true;
  if (current.inv_switch_state != last.inv_switch_state) return true;
  if (current.inv_command_known != last.inv_command_known) return true;
  if (current.grid_state_known != last.grid_state_known || current.grid_online != last.grid_online) return true;
  if (current.grid_data_fresh != last.grid_data_fresh || current.grid_power_fresh != last.grid_power_fresh) return true;
  if (current.pv_data_fresh != last.pv_data_fresh || current.battery_data_fresh != last.battery_data_fresh || current.inverter_temp_fresh != last.inverter_temp_fresh) return true;
  if (strncmp(current.grid_state, last.grid_state, sizeof(current.grid_state)) != 0) return true;
  if (strncmp(current.bat_status, last.bat_status, sizeof(current.bat_status)) != 0) return true;
  return false;
}

bool readU32BigEndianFromResponse(uint8_t responseWords, uint8_t highIndex, uint8_t lowIndex, uint32_t &value) {
  if (highIndex >= responseWords || lowIndex >= responseWords) { value = 0; return false; }
  value = ((uint32_t)node.getResponseBuffer(highIndex) << 16) | node.getResponseBuffer(lowIndex);
  return true;
}

void resetModbusUart() {
  Serial2.end();
  delayWithWatchdog(80);
  Serial2.setRxBufferSize(256);
  Serial2.begin(19200, SERIAL_8N1, RX2_PIN, TX2_PIN);
  Serial2.setTimeout(UART_STREAM_TIMEOUT_MS);
  node.begin(1, Serial2);
  hardwareClearUART(true);
}

void recoverModbusIfNeeded() {
  if (consecutiveModbusErrors >= MAX_MODBUS_ERRORS) {
    stat_uartResets++;
    resetModbusUart();
    consecutiveModbusErrors = 0;
  }
}

bool isWifiGotIpEvent(WiFiEvent_t event) {
#if defined(ESP_ARDUINO_VERSION_MAJOR) && ESP_ARDUINO_VERSION_MAJOR >= 3
  return event == ARDUINO_EVENT_WIFI_STA_GOT_IP;
#else
  return event == SYSTEM_EVENT_STA_GOT_IP;
#endif
}

void configureTaskWatchdog() {
#if defined(ESP_ARDUINO_VERSION_MAJOR) && ESP_ARDUINO_VERSION_MAJOR >= 3
  esp_task_wdt_config_t c = {}; c.timeout_ms = WATCHDOG_TIMEOUT_MS; c.idle_core_mask = (1 << portNUM_PROCESSORS) - 1; c.trigger_panic = true;
  if (esp_task_wdt_reconfigure(&c) != ESP_OK) (void)esp_task_wdt_init(&c);
#else
  (void)esp_task_wdt_init(WATCHDOG_TIMEOUT_MS / 1000, true);
#endif
}

bool safeFirebaseUpdate(FirebaseData &fb, const char *path, FirebaseJson &json) {
  if (WiFi.status() != WL_CONNECTED || !Firebase.ready()) return false;
  bool success = false;
  for (uint8_t retry = 0; retry < FIREBASE_UPDATE_RETRIES; retry++) {
    core0_heartbeat.store(millis());
    feedWatchdog();
    success = Firebase.updateNodeSilent(fb, path, json);
    core0_heartbeat.store(millis());
    feedWatchdog();
    if (success) break;
    DLOGF("[FIREBASE] Update error %s: %s\n", path, fb.errorReason().c_str());
    if (retry + 1 < FIREBASE_UPDATE_RETRIES) delayWithWatchdog(80);
  }
  
  if (success) {
    firebaseConsecutiveFails = 0;
    stat_lastFirebaseSuccessTime.store(millis());
  } else {
    int fails = ++firebaseConsecutiveFails;
    if (fails >= 3) {
      DLOG("[FIREBASE] 3 lan update loi -> Don dep socket fbdo de tu thong lai!\n");
      fb.clear();
      firebaseConsecutiveFails = 0;
    }
  }
  return success;
}

void executePeriodicEnergySnapshot(const EnergyData_t &energy) {
  if (WiFi.status() != WL_CONNECTED || !Firebase.ready()) return;

  struct tm timeinfo;
  if (!getLocalTime(&timeinfo, 1000)) return;
  if (timeinfo.tm_year < 120) return; 

  char monthKey[16], dayKey[16];
  snprintf(monthKey, sizeof(monthKey), "%04d_%02d", timeinfo.tm_year + 1900, timeinfo.tm_mon + 1);
  snprintf(dayKey, sizeof(dayKey), "day_%02d", timeinfo.tm_mday);

  FirebaseJson jsonDay;
  jsonDay.set("pv", roundf(energy.pv_today * 10.0f) / 10.0f);
  jsonDay.set("load", roundf(energy.load_today * 10.0f) / 10.0f);
  jsonDay.set("grid", roundf(energy.grid_buy_today * 10.0f) / 10.0f);
  jsonDay.set("batt", roundf(energy.bat_discharge_today * 10.0f) / 10.0f);
  jsonDay.set("charge", roundf(energy.bat_charge_today * 10.0f) / 10.0f);

  String dailyPath = String("/SosenSystem/History/Daily/") + monthKey + "/" + dayKey;
  if (safeFirebaseUpdate(fbdo, dailyPath.c_str(), jsonDay)) {
    stat_historySnapshots++;
    DLOGF("[HISTORY] Da chot so lieu: %s/%s\n", monthKey, dayKey);
  }
}

// ================= HÀM XỬ LÝ NẠP OTA QUA HTTPS/HTTP =================
void handleRemoteOTA(const String &firmwareUrl) {
  if (firmwareUrl.length() < 10) return;

  DLOGF("[OTA] Khoi dong qua trinh nap tu xa: %s\n", firmwareUrl.c_str());
  isUpdatingOTA.store(true);

  // 1. Dọn dẹp Stream và socket fbdo để giải phóng RAM
  Firebase.endStream(streamData);
  fbdo.clear();
  delay(100);

  // 2. Báo cáo trạng thái bắt đầu nạp lên Firebase
  Firebase.setString(fbdo, "/SosenSystem/Control/OTA_Status", "DOWNLOADING");
  Firebase.setInt(fbdo, "/SosenSystem/Control/OTA_Progress", 0);

  // 3. Cài đặt HTTPUpdate
  httpUpdate.setLedPin(LED_PIN, LOW);
  httpUpdate.rebootOnUpdate(false);

  httpUpdate.onProgress([](int current, int total) {
    feedWatchdog();
    core0_heartbeat.store(millis());
    core1_heartbeat.store(millis());
    if (total > 0) {
      int percent = (current * 100) / total;
      static int lastPercent = -1;
      if (percent % 10 == 0 && percent != lastPercent) {
        lastPercent = percent;
        DLOGF("[OTA] Tien trinh: %d%% (%d/%d bytes)\n", percent, current, total);
      }
    }
  });

  WiFiClientSecure secureClient;
  secureClient.setInsecure(); // Chấp nhận mọi chứng chỉ SSL (GitHub, CDN...)

  t_httpUpdate_return ret;
  if (firmwareUrl.startsWith("https://")) {
    ret = httpUpdate.update(secureClient, firmwareUrl);
  } else {
    WiFiClient standardClient;
    ret = httpUpdate.update(standardClient, firmwareUrl);
  }

  // 4. Kết quả nạp OTA
  switch (ret) {
    case HTTP_UPDATE_FAILED: {
      String err = httpUpdate.getLastErrorString();
      DLOGF("[OTA] That bai! Ma loi (%d): %s\n", httpUpdate.getLastError(), err.c_str());
      Firebase.setString(fbdo, "/SosenSystem/Control/OTA_Status", String("FAILED: ") + err);
      Firebase.setString(fbdo, "/SosenSystem/Control/OTA_URL", ""); // Xóa URL lỗi
      isUpdatingOTA.store(false);
      break;
    }
    case HTTP_UPDATE_NO_UPDATES:
      DLOG("[OTA] Khong co ban cap nhat");
      Firebase.setString(fbdo, "/SosenSystem/Control/OTA_Status", "NO_UPDATES");
      Firebase.setString(fbdo, "/SosenSystem/Control/OTA_URL", "");
      isUpdatingOTA.store(false);
      break;
    case HTTP_UPDATE_OK:
      DLOG("[OTA] Nap thanh cong! Restarting...");
      Firebase.setString(fbdo, "/SosenSystem/Control/OTA_Status", "SUCCESS");
      Firebase.setInt(fbdo, "/SosenSystem/Control/OTA_Progress", 100);
      Firebase.setString(fbdo, "/SosenSystem/Control/OTA_URL", "");
      delay(1000);
      ESP.restart();
      break;
  }
}

// ================= TASK CORE 0: FIREBASE & GIÁM SÁT MẠNG =================
void FirebaseTask(void * pvParameters) {
  esp_task_wdt_add(NULL); 
  fbdo.setBSSLBufferSize(4096, 1024);
  fbdo.setResponseSize(2048);
  
  streamData.setBSSLBufferSize(4096, 1024);
  streamData.setResponseSize(1024);
  
  unsigned long lastHealthCheck = 0;
  unsigned long lastStreamRetry = 0;
  unsigned long lastHeartbeatTime = 0;
  unsigned long lastStreamRefreshTime = millis();
  unsigned long wifiOfflineSince = 0;

  bool streamReady = false;
  bool healthPending = false;
  bool ignoreNextStreamValue = false;
  
  for(;;) {
    unsigned long currentMillis = millis();
    core0_heartbeat.store(currentMillis);
    feedWatchdog();

    // 1. Giám sát cạn kiệt RAM (<20KB thì restart chủ động)
    if (ESP.getFreeHeap() < 20480 && !isUpdatingOTA.load()) { 
       DLOG("[FATAL] Can kiet RAM (<20KB). Restart chu dong.");
       delayWithWatchdog(500);
       ESP.restart();
    }

    // 2. Giám sát chéo Core 1
    if (currentMillis - core1_heartbeat.load() > CORE_HEARTBEAT_TIMEOUT_MS && !isUpdatingOTA.load()) { 
      DLOG("[FATAL] Core 1 Loop treo. Restarting...");
      ESP.restart();
    }

    // 3. LIVELOCK WATCHDOG: Giám sát kẹt phiên Firebase quá 120s
    unsigned long lastOk = stat_lastFirebaseSuccessTime.load();
    if (WiFi.status() == WL_CONNECTED && lastOk != 0 && (currentMillis - lastOk > FIREBASE_STUCK_TIMEOUT_MS) && !isUpdatingOTA.load()) {
      DLOG("[FATAL] Firebase bi treo qua 120s -> Tu dong Restart trong 3s de phuc hoi!");
      delayWithWatchdog(500);
      ESP.restart();
    }
    
    // 4. Giám sát mất mạng WiFi quá 10 phút
    if (WiFi.status() != WL_CONNECTED) {
      if (wifiOfflineSince == 0) wifiOfflineSince = currentMillis;
      if (streamReady) {
        Firebase.endStream(streamData);
        streamReady = false;
        ignoreNextStreamValue = false;
      }
      if (currentMillis - wifiOfflineSince >= WIFI_OFFLINE_RESTART_MS) {
        ESP.restart();
      }
      delayWithWatchdog(1000);
      continue;
    }
    wifiOfflineSince = 0;

    // 5. Kiểm tra Firebase Ready
    if (!Firebase.ready()) {
      delayWithWatchdog(200);
      continue;
    }
    
    // 6. Làm mới Stream sau mỗi 1 giờ
    if (currentMillis - lastStreamRefreshTime >= 3600000UL) {
      lastStreamRefreshTime = currentMillis;
      if (streamReady) {
        Firebase.endStream(streamData);
        streamReady = false;
      }
    }

    // ====== LẮNG NGHE TỰ ĐỘNG STREAM TRÊN /SosenSystem/Control ======
    // Không dùng polling getString -> Tránh hoàn toàn lỗi nghẽn socket!
    if (!streamReady && (lastStreamRetry == 0 || currentMillis - lastStreamRetry >= FIREBASE_STREAM_RETRY_INTERVAL)) {
      lastStreamRetry = currentMillis;
      streamReady = Firebase.beginStream(streamData, "/SosenSystem/Control");
      if (streamReady) {
        ignoreNextStreamValue = true;
      }
    }
    
    if (streamReady) {
      if (Firebase.readStream(streamData)) {
        if (streamData.streamAvailable()) {
          bool skipInitialStreamValue = ignoreNextStreamValue;
          ignoreNextStreamValue = false;
          String streamPath = streamData.dataPath();

          // TH 1: Sự kiện lệnh Bật/Tắt biến tần
          if (streamPath == "/Inv_Switch") {
            int dType = streamData.dataTypeEnum(); 
            if (dType == fb_esp_rtdb_data_type_integer || dType == fb_esp_rtdb_data_type_float) {
              int incomingCmd = streamData.intData();
              if (isValidInverterCommand(incomingCmd) && !skipInitialStreamValue) {
                InverterState currentSwitch = getCurrentLiveSnapshot().inv_switch_state;
                if (incomingCmd != static_cast<int>(currentSwitch) && !isCommandPending.load()) {
                  isCommandPending.store(true);
                  if (xQueueOverwrite(commandQueue, &incomingCmd) != pdPASS) {
                    stat_commandDrops++;
                    isCommandPending.store(false);
                  }
                }
              }
            }
          }
          // TH 2: Sự kiện có link OTA mới gửi vào OTA_URL
          else if (streamPath == "/OTA_URL" && !skipInitialStreamValue) {
            String incomingUrl = streamData.stringData();
            incomingUrl.trim();
            if (incomingUrl.length() > 10 && (incomingUrl.startsWith("http://") || incomingUrl.startsWith("https://"))) {
              DLOGF("[OTA] Stream nhan link OTA: %s\n", incomingUrl.c_str());
              handleRemoteOTA(incomingUrl);
              continue;
            }
          }
        }
      } else {
        int code = streamData.httpCode();
        if (code != 200 && code != 0) {
          Firebase.endStream(streamData); 
          streamReady = false;
        }
      }
    }

    bool tokenConsumed = false; 

    // ====== ƯU TIÊN 1: HEARTBEAT (BẢO ĐẢM KHÔNG BỊ TRỄ BỞI LIVE DATA) ======
    if (currentMillis - lastHeartbeatTime >= HEARTBEAT_INTERVAL || lastHeartbeatTime == 0) {
      FirebaseJson jsonHb;
      jsonHb.set("Device_Heartbeat", currentMillis);
      jsonHb.set("ESP32_Online", true);
      jsonHb.set("ESP32_Status", "ONLINE");
      jsonHb.set("ESP32_Offline_After_ms", 20000);

      LiveData_t liveSnap = getCurrentLiveSnapshot();
      jsonHb.set("Grid_Online", liveSnap.grid_online);
      jsonHb.set("Grid_Status", liveSnap.grid_state);
      jsonHb.set("Grid_Raw_State", liveSnap.grid_raw_state);
      jsonHb.set("Grid_State_Known", liveSnap.grid_state_known);
      jsonHb.set("Grid_Data_Fresh", liveSnap.grid_data_fresh);
      jsonHb.set("Grid_Power_Fresh", liveSnap.grid_power_fresh);

      bool heartbeatOk = safeFirebaseUpdate(fbdo, "/SosenSystem/DeviceStatus", jsonHb);
      if (heartbeatOk) {
        lastHeartbeatTime = millis();
        tokenConsumed = true;
      }
    }
    
    // ====== ƯU TIÊN 2: LIVE DATA ======
    LiveData_t liveData;
    if (!tokenConsumed && xQueueReceive(liveQueue, &liveData, 0) == pdPASS) {
      if (hasInitialModbusSuccess.load()) {
        FirebaseJson jsonLive;
        jsonLive.set("Power_Unit", "W");
        jsonLive.set("PV_Power", liveData.pv_power);
        jsonLive.set("Grid_Power", liveData.grid_power);
        jsonLive.set("Grid_Import_Power", liveData.grid_import_power);
        jsonLive.set("Grid_Export_Power", liveData.grid_export_power);
        jsonLive.set("V_Grid", liveData.grid_voltage);
        jsonLive.set("F_Grid", liveData.grid_frequency);
        jsonLive.set("Batt_Voltage", liveData.bat_voltage);
        jsonLive.set("Batt_Current", liveData.bat_current);
        jsonLive.set("Batt_Power", liveData.bat_power);
        jsonLive.set("Batt_SOC", liveData.bat_soc);
        jsonLive.set("Batt_Temp", liveData.bat_temp); 
        jsonLive.set("Batt_Status", liveData.bat_status);
        jsonLive.set("Load_Power", liveData.load_power);
        jsonLive.set("Load_Power_Raw", liveData.load_power_raw);
        jsonLive.set("Load_Calculation_Valid", liveData.load_calculation_valid);
        jsonLive.set("Inv_Temp", liveData.inv_temp);
        jsonLive.set("Inv_Switch", static_cast<int>(liveData.inv_switch_state));
        jsonLive.set("Inv_Command_Known", liveData.inv_command_known);
        jsonLive.set("Grid_State", liveData.grid_state);
        jsonLive.set("Grid_Raw_State", liveData.grid_raw_state);
        jsonLive.set("Grid_State_Known", liveData.grid_state_known);
        jsonLive.set("Grid_Data_Fresh", liveData.grid_data_fresh);
        jsonLive.set("Grid_Power_Fresh", liveData.grid_power_fresh);
        jsonLive.set("Grid_Online", liveData.grid_online);
        jsonLive.set("PV_Data_Fresh", liveData.pv_data_fresh);
        jsonLive.set("Battery_Data_Fresh", liveData.battery_data_fresh);
        jsonLive.set("Inverter_Temp_Fresh", liveData.inverter_temp_fresh);

        if (safeFirebaseUpdate(fbdo, "/SosenSystem/Live", jsonLive)) {
           lastUploadedData = liveData;
           stat_lastUploadTime.store(millis());
        }
        tokenConsumed = true;
      }
    }

    // ====== ƯU TIÊN 3: DAILY ENERGY UPDATE ======
    EnergyData_t energyData;
    if (!tokenConsumed && xQueueReceive(energyQueue, &energyData, 0) == pdPASS) {
      FirebaseJson jsonE;
      jsonE.set("Energy_Unit", "kWh");
      jsonE.set("PV_Today", energyData.pv_today);
      jsonE.set("Grid_Buy_Today", energyData.grid_buy_today); 
      jsonE.set("Grid_Sell_Today", energyData.grid_sell_today);
      jsonE.set("Bat_Discharge_Today", energyData.bat_discharge_today);
      jsonE.set("Bat_Charge_Today", energyData.bat_charge_today);
      jsonE.set("Load_Today", energyData.load_today);
      jsonE.set("PV_Total_Lifetime", energyData.pv_total);
      jsonE.set("Grid_Buy_Total_Lifetime", energyData.grid_buy_total);
      jsonE.set("Grid_Sell_Total_Lifetime", energyData.grid_sell_total);
      jsonE.set("Load_Total_Lifetime", energyData.load_total);
      jsonE.set("Bat_Discharge_Total_Lifetime", energyData.bat_discharge_total);
      jsonE.set("Bat_Charge_Total_Lifetime", energyData.bat_charge_total);
      
      bool energyOk = safeFirebaseUpdate(fbdo, "/SosenSystem/Daily_Energy", jsonE);
      tokenConsumed = energyOk;
    }

    // ====== ƯU TIÊN 4: CHỐT LỊCH SỬ NĂNG LƯỢNG ======
    struct tm nowTm;
    if (!tokenConsumed && getLocalTime(&nowTm, 0)) {
      bool intervalDue = (lastHistorySaveMillis == 0 || (currentMillis - lastHistorySaveMillis >= HISTORY_SNAPSHOT_INTERVAL_MS));
      bool midnightDue = (nowTm.tm_hour == 23 && nowTm.tm_min >= 55 && lastMidnightSnapshotDay != nowTm.tm_mday);

      if (intervalDue || midnightDue) {
        EnergyData_t eSnap = getEnergySnapshot();
        if (eSnap.pv_today > 0 || eSnap.load_today > 0 || eSnap.grid_buy_today > 0 || eSnap.bat_discharge_today > 0) {
          lastHistorySaveMillis = currentMillis;
          if (midnightDue) lastMidnightSnapshotDay = nowTm.tm_mday;
          executePeriodicEnergySnapshot(eSnap);
          tokenConsumed = true;
        }
      }
    }
    
    // ====== ƯU TIÊN 5: SYSTEM HEALTH ======
    bool healthDue = healthPending || currentMillis - lastHealthCheck >= 600000 || lastHealthCheck == 0;
    if (healthDue) healthPending = true;
    
    if (!tokenConsumed && healthDue) {
      FirebaseJson jsonHealth;
      jsonHealth.set("Free_Heap_KB", ESP.getFreeHeap() / 1024.0);
      jsonHealth.set("Uptime_Hours", (currentMillis / 3600000.0));
      jsonHealth.set("Boot_Count", bootCount);
      jsonHealth.set("Power_Unit", "W");
      jsonHealth.set("Modbus_Success", stat_successRead.load());
      jsonHealth.set("Modbus_CRC_Errors", stat_crcError.load());
      jsonHealth.set("Modbus_Timeouts", stat_timeoutError.load());
      jsonHealth.set("WiFi_Reconnects", stat_wifiReconnects.load());
      jsonHealth.set("Firebase_Recoveries", stat_firebaseRecoveries.load());
      jsonHealth.set("UART_Resets", stat_uartResets.load());
      jsonHealth.set("History_Snapshots", stat_historySnapshots.load());
      jsonHealth.set("Last_Firebase_OK_Millis", stat_lastFirebaseSuccessTime.load());
      jsonHealth.set("Modbus_Write_Success", stat_successWrite.load());
      jsonHealth.set("Mutex_Timeouts", stat_mutexTimeouts.load());
      jsonHealth.set("Command_Drops", stat_commandDrops.load());
      jsonHealth.set("Modbus_Cycles", stat_modbusCycles.load());
      jsonHealth.set("Modbus_Cycle_Errors", stat_modbusCycleErrors.load());
      jsonHealth.set("Energy_Read_Errors", stat_energyReadErrors.load());
      jsonHealth.set("Last_Grid_Good_Millis", stat_lastGridGoodTime.load());
      jsonHealth.set("Last_PV_Good_Millis", stat_lastPvGoodTime.load());
      jsonHealth.set("Last_Battery_Good_Millis", stat_lastBatteryGoodTime.load());
      jsonHealth.set("Last_Inverter_Temp_Good_Millis", stat_lastInverterTempGoodTime.load());
      jsonHealth.set("Largest_Free_Block", heap_caps_get_largest_free_block(MALLOC_CAP_8BIT));
      jsonHealth.set("Min_Free_Heap_KB", ESP.getMinFreeHeap() / 1024.0);

      bool healthOk = safeFirebaseUpdate(fbdo, "/SosenSystem/System_Health", jsonHealth);
      if (healthOk) {
        lastHealthCheck = millis();
        healthPending = false;
        tokenConsumed = true;
      } else {
        healthPending = true;
      }
    }
    
    vTaskDelay(pdMS_TO_TICKS(80)); 
  }
}

// ================= CORE 1: ĐỘNG CƠ MODBUS RTU =================
void updateGridState(LiveData_t &d, uint32_t now, bool ok) {
  if (!ok) {
    d.grid_data_fresh = false; d.grid_power_fresh = false; d.grid_state_known = false; d.grid_online = false;
    strncpy(d.grid_state, "STALE", sizeof(d.grid_state) - 1); d.grid_state[sizeof(d.grid_state) - 1] = '\0';
    strncpy(d.grid_raw_state, "STALE", sizeof(d.grid_raw_state) - 1); d.grid_raw_state[sizeof(d.grid_raw_state) - 1] = '\0';
    gridDebounceCounter = 0;
    return;
  }
  d.grid_data_fresh = true; d.grid_state_known = true;
  bool rawOnline = (d.grid_voltage >= 180.0f && d.grid_voltage <= 260.0f && d.grid_frequency >= 45.0f && d.grid_frequency <= 55.0f);
  bool rawOff = (d.grid_voltage < 50.0f);
  const char* rawState = rawOnline ? "ON" : (rawOff ? "OFF" : "UNKNOWN");
  
  strncpy(d.grid_raw_state, rawState, sizeof(d.grid_raw_state) - 1);
  d.grid_raw_state[sizeof(d.grid_raw_state) - 1] = '\0';

  if (strcmp(rawState, lastRawGridState) == 0) {
    gridDebounceCounter++;
  } else {
    strncpy(lastRawGridState, rawState, sizeof(lastRawGridState) - 1);
    gridDebounceCounter = 1;
  }

  if (gridDebounceCounter >= 3) {
    strncpy(d.grid_state, rawState, sizeof(d.grid_state) - 1);
    d.grid_state[sizeof(d.grid_state) - 1] = '\0';
    d.grid_online = (strcmp(d.grid_state, "ON") == 0);
  }

  if (d.grid_online) stat_lastGridGoodTime.store(now);
}

void readModbusBatch() {
  stat_modbusCycles++;
  modbusPollCounter++;
  const uint32_t cycleStart = millis();
  const uint8_t attempts = getModbusMaxAttempts();
  bool cycleError = false, gridReadOk = false, gridPowerOk = false, pvOk = false, batOk = false, tempOk = true;
  LiveData_t d = getCurrentLiveSnapshot();

  // 1) Điện áp & Tần số lưới (Thanh ghi 40141)
  uint8_t r = readInverterRegistersWithRetry(40141, 2, attempts);
  if (r == node.ku8MBSuccess) {
    d.grid_frequency = node.getResponseBuffer(0) * GRID_FREQUENCY_SCALE;
    d.grid_voltage = node.getResponseBuffer(1) * GRID_VOLTAGE_SCALE;
    gridReadOk = true;
  } else {
    cycleError = true;
  }
  updateGridState(d, cycleStart, gridReadOk);

  // 2) Công suất lưới (Thanh ghi 40145)
  if (gridReadOk && strcmp(d.grid_raw_state, "OFF") != 0 && d.grid_online) {
    r = readInverterRegistersWithRetry(40145, 1, attempts);
    if (r == node.ku8MBSuccess) {
      setGridPowerW(d, normalizeGridPowerSign((int16_t)node.getResponseBuffer(0) * GRID_POWER_SCALE_W));
      gridPowerOk = true;
    } else {
      cycleError = true;
    }
  } else if (gridReadOk && strcmp(d.grid_raw_state, "OFF") == 0) {
    setGridPowerW(d, 0.0f);
    gridPowerOk = true;
    d.grid_frequency = 0.0f;
  }
  d.grid_power_fresh = gridPowerOk;

  // 3) Công suất PV (Thanh ghi 40049)
  r = readInverterRegistersWithRetry(40049, 1, attempts);
  if (r == node.ku8MBSuccess) {
    d.pv_power = node.getResponseBuffer(0) * PV_POWER_SCALE_W;
    pvOk = true; stat_lastPvGoodTime.store(cycleStart);
  } else {
    cycleError = true;
  }

  // 4) Khối Pin lưu trữ (Thanh ghi 40291)
  r = readInverterRegistersWithRetry(40291, 6, attempts);
  if (r == node.ku8MBSuccess) {
    d.bat_voltage = node.getResponseBuffer(0) * 0.1f;
    d.bat_current = (int16_t)node.getResponseBuffer(1) * 0.1f;
    d.bat_soc = node.getResponseBuffer(3);
    d.bat_temp = (int16_t)node.getResponseBuffer(5) * 0.1f;
    d.bat_power = d.bat_voltage * d.bat_current;
    batOk = true; stat_lastBatteryGoodTime.store(cycleStart);
    if (d.bat_current < -0.5f) setBatteryStatus(d, "Dang Sac");
    else if (d.bat_current > 0.5f) setBatteryStatus(d, "Dang Xa");
    else setBatteryStatus(d, "Cho");
  } else {
    cycleError = true;
  }

  // 5) Công suất Tải
  if (gridPowerOk && pvOk && batOk) {
    d.load_power_raw = d.pv_power + d.bat_power - d.grid_power;
    if (d.load_power_raw < -100.0f) {
      d.load_calculation_valid = false;
      d.load_power = 0.0f;
    } else {
      d.load_calculation_valid = true;
      d.load_power = (d.load_power_raw < 0.0f) ? 0.0f : d.load_power_raw;
    }
  } else {
    d.load_calculation_valid = false;
  }

  // 6) Nhiệt độ Inverter (40259)
  if (modbusPollCounter % 4 == 0 || modbusPollCounter == 1) {
    tempOk = false;
    r = readInverterRegistersWithRetry(40259, 1, attempts);
    if (r == node.ku8MBSuccess) { 
      d.inv_temp = (int16_t)node.getResponseBuffer(0) * 0.1f; 
      tempOk = true; 
      stat_lastInverterTempGoodTime.store(cycleStart); 
    } else {
      cycleError = true;
    }
  }

  // 7) Công tắc 50031
  if (modbusPollCounter % 450 == 22 || modbusPollCounter == 2) {
    uint8_t resSw = readHoldingRegistersWithRetry(50031, 1, attempts);
    if (resSw == node.ku8MBSuccess) {
      setCurrentInvSwitchState((node.getResponseBuffer(0) == 1) ? INV_ON : INV_OFF);
    } else if (d.pv_power > 50.0f || fabsf(d.bat_power) > 50.0f) {
      setCurrentInvSwitchState(INV_ON);
    }
  }

  d.grid_data_fresh = gridReadOk; d.pv_data_fresh = pvOk; d.battery_data_fresh = batOk;
  d.inverter_temp_fresh = tempOk || (millis() - stat_lastInverterTempGoodTime.load() < INVERTER_DATA_STALE_MS);
  if (!gridReadOk) updateGridState(d, cycleStart, false);

  if (batOk || pvOk || gridReadOk) {
    hasInitialModbusSuccess.store(true);
  }

  if (isValidData(d)) { 
    commitCurrentLive(d); 
    if (gridReadOk && gridPowerOk && pvOk && batOk) stat_lastReadTime.store(millis()); 
    triggerBlink(); 
  }
  
  if (cycleError) { consecutiveModbusErrors++; stat_modbusCycleErrors++; } 
  else { consecutiveModbusErrors = 0; }
  recoverModbusIfNeeded();

  // 8) Khối năng lượng tích lũy (40016)
  if (modbusPollCounter % 75 == 0 || modbusPollCounter == 1) {
    r = readInverterRegistersWithRetry(40016, ENERGY_REGISTER_COUNT, attempts);
    if (r == node.ku8MBSuccess) {
      EnergyData_t e{};
      e.pv_today            = node.getResponseBuffer(0) * 0.1f; 
      e.grid_buy_today      = node.getResponseBuffer(1) * 0.1f; 
      e.grid_sell_today     = node.getResponseBuffer(2) * 0.1f; 
      e.bat_discharge_today = node.getResponseBuffer(3) * 0.1f; 
      e.bat_charge_today    = node.getResponseBuffer(4) * 0.1f; 
      e.load_today          = node.getResponseBuffer(5) * 0.1f;

      uint32_t a = 0, b = 0, c = 0, dv = 0, e1 = 0, e2 = 0;
      bool ok = readU32BigEndianFromResponse(ENERGY_REGISTER_COUNT, 7, 8, a) &&
                readU32BigEndianFromResponse(ENERGY_REGISTER_COUNT, 9, 10, b) &&
                readU32BigEndianFromResponse(ENERGY_REGISTER_COUNT, 11, 12, c) &&
                readU32BigEndianFromResponse(ENERGY_REGISTER_COUNT, 17, 18, dv) &&
                readU32BigEndianFromResponse(ENERGY_REGISTER_COUNT, 13, 14, e1) &&
                readU32BigEndianFromResponse(ENERGY_REGISTER_COUNT, 15, 16, e2);
      if (ok) { 
        e.pv_total = a * 0.1f; 
        e.grid_buy_total = b * 0.1f; 
        e.grid_sell_total = c * 0.1f; 
        e.load_total = dv * 0.1f; 
        e.bat_discharge_total = e1 * 0.1f; 
        e.bat_charge_total = e2 * 0.1f; 
        if (isValidEnergyData(e)) { 
          commitEnergySnapshot(e); 
          xQueueOverwrite(energyQueue, &e); 
        } else {
          stat_energyReadErrors++;
        }
      } else {
        stat_energyReadErrors++;
      }
    } else {
      stat_energyReadErrors++;
    }
  }
}

// ================= SETUP HỆ THỐNG =================
void setup() {
  WRITE_PERI_REG(RTC_CNTL_BROWN_OUT_REG, 0);
  setCpuFrequencyMhz(160);
  delay(200);

#if DEBUG_MODE
  Serial.begin(115200);
  delay(200); 
  DLOG("\n[BOOT] SOSEN ESP32 V3.9-STABLE REMOTE OTA ACTIVE");
#endif
  
  bootCount++; 
  resetReason = esp_reset_reason();
  DLOGF("[BOOT] Count=%d ResetReason=%d CPU_Freq=%dMHz\n", bootCount, resetReason, getCpuFrequencyMhz());

  liveMutex = xSemaphoreCreateMutex(); 
  energyMutex = xSemaphoreCreateMutex();
  
  pinMode(LED_PIN, OUTPUT);
  pinMode(TRIGGER_PIN, INPUT_PULLUP);
  memset(&currentLive, 0, sizeof(LiveData_t));
  setBatteryStatus(currentLive, "Khoi dong");
  currentLive.inv_switch_state = INV_OFF;
  currentLive.inv_command_known = false;
  currentLive.grid_state_known = false; currentLive.grid_data_fresh = false; currentLive.grid_power_fresh = false; currentLive.grid_online = false;
  currentLive.pv_data_fresh = false; currentLive.battery_data_fresh = false; currentLive.inverter_temp_fresh = false;
  currentLive.load_calculation_valid = false;
  strncpy(currentLive.grid_state, "UNKNOWN", sizeof(currentLive.grid_state) - 1); 
  strncpy(currentLive.grid_raw_state, "UNKNOWN", sizeof(currentLive.grid_raw_state) - 1);

  configureTaskWatchdog();

  Serial2.setRxBufferSize(256);
  Serial2.begin(19200, SERIAL_8N1, RX2_PIN, TX2_PIN);
  Serial2.setTimeout(UART_STREAM_TIMEOUT_MS); 
  node.begin(1, Serial2); 
  
  liveQueue = xQueueCreate(1, sizeof(LiveData_t));
  energyQueue = xQueueCreate(1, sizeof(EnergyData_t));
  commandQueue = xQueueCreate(COMMAND_QUEUE_LENGTH, sizeof(int));
  if (liveQueue == NULL || energyQueue == NULL || commandQueue == NULL || liveMutex == NULL || energyMutex == NULL) {
    delay(1000);
    ESP.restart();
  }
  
  WiFi.mode(WIFI_STA);
  WiFi.setTxPower(WIFI_POWER_17dBm); 
  WiFi.setAutoReconnect(true); 
  WiFi.onEvent([](WiFiEvent_t event, WiFiEventInfo_t info){
      if (isWifiGotIpEvent(event)) {
          esp_wifi_set_ps(WIFI_PS_NONE);
      }
  });
  
  preferences.begin("sosen_cfg", false);
  saved_wifi_ssid = preferences.getString("wifi_ssid", "");
  saved_wifi_pass = preferences.getString("wifi_pass", "");
  global_fb_host  = preferences.getString("fb_host", DEFAULT_FIREBASE_HOST);
  global_fb_auth  = preferences.getString("fb_auth", DEFAULT_FIREBASE_AUTH);
  
  WiFi.disconnect(false);
  delay(100);

  if (saved_wifi_ssid != "") {
      DLOGF("[WIFI] Dang ket noi vao WiFi da luu: %s\n", saved_wifi_ssid.c_str());
      WiFi.begin(saved_wifi_ssid.c_str(), saved_wifi_pass.c_str());
  } else {
      DLOG("[WIFI] Thu ket noi bang NVS mac dinh...");
      WiFi.begin();
  }
  
  unsigned long startWait = millis();
  while (WiFi.status() != WL_CONNECTED && millis() - startWait < 10000) {
      delay(500);
  }

  if (WiFi.status() != WL_CONNECTED || digitalRead(TRIGGER_PIN) == LOW) {
      DLOG("[WIFI] Mo che do cai dat SOSEN_MASTER...");
      WiFiManager wm;
      wm.setConnectTimeout(20);
      wm.setConfigPortalTimeout(180);
      
      WiFiManagerParameter custom_fb_host("fb_host", "Firebase DB URL", global_fb_host.c_str(), 100);
      WiFiManagerParameter custom_fb_auth("fb_auth", "Firebase DB Secret", global_fb_auth.c_str(), 100);
      wm.addParameter(&custom_fb_host);
      wm.addParameter(&custom_fb_auth);
      
      if (!wm.startConfigPortal("SOSEN_MASTER")) {
          ESP.restart();
      }
      
      preferences.putString("wifi_ssid", WiFi.SSID());
      preferences.putString("wifi_pass", WiFi.psk());
      if (String(custom_fb_host.getValue()) != "" && String(custom_fb_auth.getValue()) != "") {
         preferences.putString("fb_host", custom_fb_host.getValue());
         preferences.putString("fb_auth", custom_fb_auth.getValue());
         global_fb_host = custom_fb_host.getValue();
         global_fb_auth = custom_fb_auth.getValue();
      }
      DLOG("[WIFI] Da luu thanh cong thong tin WiFi moi vao Flash!");
      ESP.restart();
  }
  
  DLOG("[WIFI] Da ket noi Router gia dinh thanh cong!");
  esp_wifi_set_ps(WIFI_PS_NONE); 
  
  configTime(GMT_OFFSET_SEC, DAYLIGHT_OFFSET_SEC, NTP_SERVER_1, NTP_SERVER_2);
  DLOG("[NTP] Da khoi tao dong bo gio Viet Nam GMT+7");
  
  config.timeout.serverResponse = 8000; 
  config.timeout.socketConnection = 8000; 
  config.timeout.networkReconnect = 3000;
  config.database_url = global_fb_host.c_str();
  config.signer.tokens.legacy_token = global_fb_auth.c_str();
  
  Firebase.begin(&config, &auth);
  Firebase.reconnectWiFi(true);
  Firebase.setFloatDigits(2);
  Firebase.setDoubleDigits(2);
  DLOG("[FIREBASE] Khoi tao Firebase RTDB thanh cong");
  
  unsigned long now = millis();
  core0_heartbeat.store(now);
  core1_heartbeat.store(now);
  stat_lastFirebaseSuccessTime.store(now);
  lastModbusCycleTime = now - CYCLE_INTERVAL_LIVE;
  
  if (xTaskCreatePinnedToCore(FirebaseTask, "FirebaseTask", 16384, NULL, 1, NULL, 0) != pdPASS) {
    DLOG("[FATAL] Khong tao duoc FirebaseTask");
    delay(1000);
    ESP.restart();
  }
  esp_task_wdt_add(NULL); 
}

// ================= MAIN LOOP (CORE 1) =================
void loop() {
  unsigned long currentMillis = millis();
  handleBlink();
  core1_heartbeat.store(currentMillis);

  // Khi đang nạp OTA: Dừng polling Modbus
  if (isUpdatingOTA.load()) {
    feedWatchdog();
    vTaskDelay(pdMS_TO_TICKS(100));
    return;
  }

  // Giám sát chéo Core 0
  if (currentMillis - core0_heartbeat.load() > CORE_HEARTBEAT_TIMEOUT_MS) {
    DLOG("[FATAL] Core 0 ngung hoat dong -> Restart ESP32");
    ESP.restart();
  }
  
  // Xử lý lệnh từ Queue
  int targetCmd;
  if (xQueueReceive(commandQueue, &targetCmd, 0) == pdPASS) {
    if (isValidInverterCommand(targetCmd)) {
      uint8_t maxAttempts = getModbusMaxAttempts();
      uint8_t res = writeInverterSingleRegisterWithRetry(50031, targetCmd, maxAttempts);
      if (res == node.ku8MBSuccess) {
          setCurrentInvSwitchState((targetCmd == 1) ? INV_ON : INV_OFF);
          consecutiveModbusErrors = 0;
      } else {
          consecutiveModbusErrors++;
          recoverModbusIfNeeded();
      }
      isCommandPending.store(false);
    } else {
      isCommandPending.store(false);
    }
  }
  
  // Chu kỳ quét Modbus
  if (currentMillis - lastModbusCycleTime >= CYCLE_INTERVAL_LIVE) {
    lastModbusCycleTime += CYCLE_INTERVAL_LIVE;
    if (currentMillis - lastModbusCycleTime >= CYCLE_INTERVAL_LIVE) {
      lastModbusCycleTime = currentMillis;
    }
    
    readModbusBatch();
    
    // Đẩy dữ liệu lên Live Queue
    if (hasInitialModbusSuccess.load()) {
      LiveData_t finalSnapshot = getCurrentLiveSnapshot();
      unsigned long lastUpload = stat_lastUploadTime.load();
      bool forceUpload = (lastUpload == 0 || currentMillis - lastUpload >= LIVE_FORCE_UPLOAD_INTERVAL);
      
      if (!hasLastReadData || isDataChanged(finalSnapshot, lastReadData) || forceUpload) {
        xQueueOverwrite(liveQueue, &finalSnapshot);
        lastReadData = finalSnapshot;
        hasLastReadData = true;
      }
    }
  }
  
  // Nút nhấn Reset WiFi (Nhấn giữ nút BOOT GPIO0 > 5s)
  static unsigned long buttonPressStart = 0;

  if (digitalRead(TRIGGER_PIN) == LOW) {
    if (buttonPressStart == 0) {
      buttonPressStart = currentMillis;
    } else if (currentMillis - buttonPressStart >= 5000) {
      DLOG("[RESET] Nhan giu nut BOOT >5s -> Xoa WiFi va mo lai SOSEN_MASTER...");
      preferences.remove("wifi_ssid");
      preferences.remove("wifi_pass");
      WiFiManager wm;
      wm.resetSettings();
      delay(500);
      ESP.restart();
    }
  } else {
    buttonPressStart = 0; 
  }
  
  feedWatchdog(); 
  vTaskDelay(pdMS_TO_TICKS(40)); 
}