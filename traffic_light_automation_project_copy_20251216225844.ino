#include <Arduino.h>
#include <WiFiNINA.h>
#include <PubSubClient.h>
#include <DHT.h>
#include <rgb_lcd.h>


#define DHTPIN 4
#define DHTTYPE DHT11


#define redE 6
#define blueE 7
#define greenE 8


#define redS 0
#define yellowS 1
#define greenS 2


const int trigE = A1;
const int echoE = 3;
const int trigS = A2;
const int echoS = 5;


// MQTT setup 
const char* ssid = "Apple";
const char* password = "jsmf1484";
const char* mqtt_server = "broker.emqx.io";
const int mqtt_port = 1883;
const char* topic_pub = "mkrtohost1";// data publishing topic 
const char* topic_sub = "host2mkr"; // ACK receiving topic 

WiFiClient wifiClient;
PubSubClient client(wifiClient);
rgb_lcd lcd;


#define max_data 50
#define ACK_timeout 1000
#define retry 5
#define alpha 0.5
#define GREEN_DURATION 12000  

enum TrafficLightState { TL_RED, TL_GREEN };
// Struck to store different variable into one program 
struct TrafficLight {
  TrafficLightState state;
  int redPin;
  int greenPin;
};

struct Message { // struck of the message package which will send to gateway 
  int id;
  float filteredE;
  float filteredS;
  float Temperature;
  float Humidity;
  float DewPoint;
  TrafficLightState stateE;
  TrafficLightState stateS;
  bool waitForACK;
};

// ------------------- Globals -------------------
DHT dht(DHTPIN, DHTTYPE);

Message Buffer[max_data];
int Bufferstart = 0;
int bufferend = 0;
int next_id = 1;
bool ackReceived = false;

float filteredE = 0;
float filteredS = 0;


TrafficLight east = {TL_RED, redE, greenE};
TrafficLight south = {TL_RED, redS, greenS};


unsigned long lastSwitchTime = 0;

// this function is used to received the message from subscribe topic 
void datafrompietomqtt(char* topic, byte* payload, unsigned int length) {
  String msg = "";
  for (int i = 0; i < length; i++) msg += (char)payload[i];
  msg.trim();
  Serial.println("Received from Python: " + msg);

  if (String(topic) == topic_sub && msg.indexOf("ACK") >= 0) {
    ackReceived = true;
    Serial.println("ACK recognized!");
  }
}

// the function mainly used to maintain the ack time if the time is over it will move into next data 
bool waitforACK() {
  ackReceived = false;
  unsigned long starttime = millis();
  while (millis() - starttime < ACK_timeout) {
    client.loop();
    if (ackReceived) return true;
  }
  return false;
}

void Message_sent() { // thsís function is used to create the message which was define in struck instering the sensor data
  int nextSlot = bufferend;
  Buffer[nextSlot].id = next_id++;
  Buffer[nextSlot].filteredE = filteredE;
  Buffer[nextSlot].filteredS = filteredS;
  Buffer[nextSlot].Temperature = dht.readTemperature();
  Buffer[nextSlot].Humidity = dht.readHumidity();
  Buffer[nextSlot].DewPoint = Buffer[nextSlot].Temperature - ((100 - Buffer[nextSlot].Humidity)/5.0);
  Buffer[nextSlot].stateE = east.state;
  Buffer[nextSlot].stateS = south.state;
  Buffer[nextSlot].waitForACK = true;
  bufferend = (bufferend + 1) % max_data;

  Serial.print("Added to buffer with ID: ");
  Serial.println(Buffer[nextSlot].id);
}

void printBufferIDs() {
  Serial.print("Buffer waiting IDs: ");
  int i = Bufferstart;
  while (i != bufferend) {
    Serial.print(Buffer[i].id); Serial.print(" ");
    i = (i + 1) % max_data;
  }
  Serial.println();
}

void Bufferedmsgtobesend() {
  int tries = 0;
  while (Bufferstart != bufferend) {
    printBufferIDs();

    String payload = "{";
    payload += "\"id\":" + String(Buffer[Bufferstart].id) + ",";
    payload += "\"filteredE\":" + String(Buffer[Bufferstart].filteredE) + ",";
    payload += "\"filteredS\":" + String(Buffer[Bufferstart].filteredS) + ",";
    payload += "\"Temperature\":" + String(Buffer[Bufferstart].Temperature) + ",";
    payload += "\"Humidity\":" + String(Buffer[Bufferstart].Humidity) + ",";
    payload += "\"DewPoint\":" + String(Buffer[Bufferstart].DewPoint) + ",";
    payload += "\"stateE\":" + String(Buffer[Bufferstart].stateE) + ",";
    payload += "\"stateS\":" + String(Buffer[Bufferstart].stateS);
    payload += "}";

    client.publish(topic_pub, payload.c_str());
    Serial.print("Sent ID: "); Serial.println(Buffer[Bufferstart].id);

    if (waitforACK()) {
      Bufferstart = (Bufferstart + 1) % max_data;
      tries = 0;
    } else {
      tries++;
      if (tries >= retry) {
        Serial.println("Max retries reached, skipping message.");
        Bufferstart = (Bufferstart + 1) % max_data;
        tries = 0;
      }
      break;
    }
  }
}

//wifi setup 
void connectWiFi() {
  Serial.println("Connecting to WiFi...");
  WiFi.begin(ssid, password);
  while (WiFi.status() != WL_CONNECTED) {
    delay(500); Serial.print(".");
  }
  Serial.println("\nWiFi connected. IP: " + String(WiFi.localIP()));
}
// mqtt setup
void connectMQTT() {
  client.setServer(mqtt_server, mqtt_port);
  client.setCallback(datafrompietomqtt);
  while (!client.connected()) {
    String clientId = "MKR_PRXI_1010";
    if (client.connect(clientId.c_str())) {
      Serial.println("Connected to MQTT");
      client.subscribe(topic_sub);
    } else {
      Serial.print("MQTT connection failed, rc=");
      Serial.println(client.state());
      delay(2000);
    }
  }
}
// function to measure the distance 
float Ultrasonic_distance(int trigPin, int echoPin, float &filteredValue) {
  digitalWrite(trigPin, LOW);
  delayMicroseconds(2);
  digitalWrite(trigPin, HIGH);
  delayMicroseconds(10);
  digitalWrite(trigPin, LOW);

  long duration = pulseIn(echoPin, HIGH, 30000); 
  float distance = (duration * 0.0343 / 2.0);
  filteredValue = alpha * distance + (1 - alpha) * filteredValue;
  return filteredValue;
}


void updateLED(TrafficLight &light) {
  if(light.state == TL_RED){
    digitalWrite(light.redPin, HIGH);
    digitalWrite(light.greenPin, LOW);
  } else {
    digitalWrite(light.redPin, LOW);
    digitalWrite(light.greenPin, HIGH);
  }
}


void setup() {
  Serial.begin(9600);
  pinMode(trigE, OUTPUT); pinMode(echoE, INPUT);
  pinMode(trigS, OUTPUT); pinMode(echoS, INPUT);
  pinMode(redE, OUTPUT); pinMode(greenE, OUTPUT);
  pinMode(redS, OUTPUT); pinMode(greenS, OUTPUT);

  dht.begin();
  lcd.begin(16,2);
  lcd.setRGB(255,255,0);
  lcd.clear();

  connectWiFi();
  connectMQTT();

  /
  east.state = TL_RED;
  south.state = TL_RED;
  lastSwitchTime = millis();
}


void loop() {
  // Read sensors
  Ultrasonic_distance(trigE, echoE, filteredE);
  Ultrasonic_distance(trigS, echoS, filteredS);

  // checking the switching time 
  unsigned long currentTime = millis();
  if(currentTime - lastSwitchTime >= GREEN_DURATION){
    // Switch green to other
    if(east.state == TL_GREEN){
      east.state = TL_RED;
      south.state = TL_GREEN;
    } else {
      east.state = TL_GREEN;
      south.state = TL_RED;
    }
    lastSwitchTime = currentTime;
  }

  // overide the condition if there is no traffic in one direction 
  if(east.state == TL_GREEN && filteredE < 10 && filteredS > 10){
    east.state = TL_RED;
    south.state = TL_GREEN;
    lastSwitchTime = currentTime;
  }
  if(south.state == TL_GREEN && filteredS < 10 && filteredE > 10){
    south.state = TL_RED;
    east.state = TL_GREEN;
    lastSwitchTime = currentTime;
  }

 
  updateLED(east);
  updateLED(south);

  // Display
  lcd.clear();
  lcd.setCursor(0,0);
  lcd.print("E:"); lcd.print(filteredE); lcd.print(" S:"); lcd.print(filteredS);
  lcd.setCursor(0,1);
  lcd.print("T:"); lcd.print(dht.readTemperature()); lcd.print(" H:"); lcd.print(dht.readHumidity());

  // Add to buffer
  int nextSlot = (bufferend + 1) % max_data;
  if(nextSlot != Bufferstart){
    Message_sent();
  } else {
    Serial.println("Buffer full, skipping message");
  }

  // Send buffered messages
  Bufferedmsgtobesend();

  // Print
  Serial.print("East: "); Serial.print(east.state==TL_GREEN?"Green":"Red");
  Serial.print(" | South: "); Serial.println(south.state==TL_GREEN?"Green":"Red");
  Serial.print("Dist E: "); Serial.print(filteredE);
  Serial.print(" S: "); Serial.println(filteredS);

  delay(1000);
}


