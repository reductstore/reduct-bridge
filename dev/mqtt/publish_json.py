import time
import random
import json
import paho.mqtt.client as mqtt

client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="json-sim")
client.connect("mosquitto", 1883)

sensors = ["temp-01", "temp-02", "humidity-01"]
locations = ["warehouse", "office", "lab"]

while True:
    ts = int(time.time() * 1000)
    for sensor, location in zip(sensors, locations):
        msg = {
            "sensor_id": sensor,
            "location": location,
            "temperature_c": round(20.0 + random.gauss(0, 2.0), 2),
            "humidity_pct": round(50.0 + random.gauss(0, 5.0), 2),
            "timestamp_ms": ts,
        }
        client.publish(
            f"json/sensors/{location}/{sensor}",
            json.dumps(msg).encode(),
            qos=0,
        )
    time.sleep(2)
