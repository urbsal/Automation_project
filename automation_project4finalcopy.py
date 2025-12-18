import paho.mqtt.client as mqtt
import json
from datetime import datetime, timezone
import math
import time
from collections import deque
import csv
from pymongo import MongoClient

mongo_client = MongoClient( # setup a connection with MONGODB
    "mongodb+srv://bsalsthabs_db_user:Mongodbpassword@cluster0.w0ueoor.mongodb.net/?appName=Cluster0"
)

Sensors_database_name = mongo_client["Sensor_Records"] #create a folder in MONGODB
collection_of_data = Sensors_database_name["Filtered_sensor_datas"]

csv_file = "filtered_data.csv"

data_sample_temperature = 5 # size of the list where the raw data is store 
data_sample_humidity = 5
data_sample_dewpoint = 5

filtered_temperature = deque(maxlen=data_sample_temperature) # first out last in for moving average filteration
filtered_humidity = deque(maxlen=data_sample_humidity)
filtered_dewpoint = deque(maxlen=data_sample_dewpoint)

data_tracker = 0

Broker = "broker.emqx.io" #mqtt connection
mqtt_port = 1883
publish_topic = "host2mkr"
receive_topic = "mkrtohost1"
dashboard_and_cloud_topic = "cleandata"

def create_id(): #create a unique id for each data set
    global data_tracker
    data_tracker += 1
    return data_tracker

def safe_append_and_average(queue, value):
    if isinstance(value, (int, float)):
        queue.append(value)
    clean = [v for v in queue if isinstance(v, (int, float))]
    return sum(clean) / len(clean) if clean else None

def on_connect(client, userdata, flags, rc): # connect with mqtt topic
    print("Connected with MQTT", rc)
    client.subscribe(receive_topic, qos=2)

def on_message(client, userdata, msg): # recive message from mqtt
    print("---- MQTT MESSAGE RECEIVED ----")
    print("Topic:", msg.topic)
    print("Payload bytes:", msg.payload) # convert the byte in string
    try:
        payload_str = msg.payload.decode("utf-8")
    except UnicodeDecodeError:
        payload_str = str(msg.payload)
    print("Decoded payload:", payload_str)

    try:
        data = json.loads(payload_str) # convert into dictionary 

        temperature = data.get("Temperature") # received indivisual data 
        humidity = data.get("Humidity")
        dew_point = data.get("DewPoint")

        avg_temp = safe_append_and_average(filtered_temperature, temperature) # filtering the data 
        avg_humidity = safe_append_and_average(filtered_humidity, humidity)
        avg_dewpoint = safe_append_and_average(filtered_dewpoint, dew_point)

        filtered_data = { # create a dictionary of the data with timestamp and id to send to mongodb and dashboard 
            "id": create_id(),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "Traffic_East": data.get("filteredE"),
            "Traffic_south": data.get("filteredS"),
            "Temperature": avg_temp,
            "Humidity": avg_humidity,
            "dew_point": avg_dewpoint,
            "State_East": data.get("stateE"),
            "State_south": data.get("stateS")
        }

        with open(csv_file, "a", newline="") as f: # store data in csv 
            writer = csv.writer(f)
            writer.writerow([
                filtered_data["timestamp"],
                filtered_data["Traffic_East"],
                filtered_data["Traffic_south"],
                filtered_data["Temperature"],
                filtered_data["Humidity"],
                filtered_data["dew_point"],
                filtered_data["State_East"],
                filtered_data["State_south"]
            ])

        collection_of_data.insert_one(filtered_data)
        filtered_data.pop("_id", None)

        # Replace NaN values with None
        for key, value in data.items():
            if isinstance(value, float) and math.isnan(value):
                data[key] = None
        data["utc_time"] = datetime.now(timezone.utc).isoformat()

        # Publish cleaned data
        client.publish(publish_topic, json.dumps(data), qos=2)# forward the dictionary 
        client.publish(dashboard_and_cloud_topic, json.dumps(filtered_data), qos=2)

        # Send ACK message
        ack_message = {
            "status": "ACK",
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        client.publish(publish_topic, json.dumps(ack_message), qos=2)

        time.sleep(0.5)

    except json.JSONDecodeError as e:
        error_ack = {
            "status": "NACK",
            "error": str(e),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        client.publish(publish_topic, json.dumps(error_ack), qos=2)

def on_publish(client, userdata, mid):
    print("Message Published:", mid)

mqtt_client = mqtt.Client()
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message
mqtt_client.on_publish = on_publish

mqtt_client.connect(Broker, mqtt_port, 60)
mqtt_client.loop_forever()
