# python3.6

import random
import sched, time
import threading
from paho.mqtt import client as mqtt_client


broker = 'broker.emqx.io'
port = 1883
topic = "/python/mqtt"
topic_1 = "/prix"
topic_2 = "/reduction"
topic_3 = "/consommation"
topic_4 = "/production"
# generate client ID with pub prefix randomly
client_id = f'sm-mqtt-{random.randint(0, 100)}'


def connect_mqtt() -> mqtt_client:
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)

    client = mqtt_client.Client(client_id)
    client.on_connect = on_connect
    client.connect(broker, port)
    return client

def publish_consommation(client):
    msg_count = 0
    msg = str({"id": client_id, "consomation":4})
    result = client.publish(topic_3, msg)
    # result: [0, 1]
    status = result[0]
    if status == 0:
        print(f"Send `{msg}` to topic `{topic_3}`")
    else:
        print(f"Failed to send message to topic {topic_3}")
    msg_count += 1
    threading.Timer(60.0*60.0, publish_consommation).start()
    

def publish_production(client):
    msg_count = 0
    msg = str({"id": client_id, "production":2})
    result = client.publish(topic_4, msg)
    # result: [0, 1]
    status = result[0]
    if status == 0:
        print(f"Send `{msg}` to topic `{topic_4}`")
    else:
        print(f"Failed to send message to topic {topic_4}")
    msg_count += 1
    threading.Timer(60.0*15.0, publish_production).start()

def subscribe(client: mqtt_client):
    def on_message(client, userdata, msg):
        print(f"Received `{msg.payload.decode()}` from `{msg.topic}` topic")

    client.subscribe(topic_1)
    client.subscribe(topic_2)
    client.on_message = on_message


def run():
    client = connect_mqtt()
    client.loop_start()
    subscribe(client)
    publish_consommation(client)
    publish_production(client)
    


if __name__ == '__main__':
    run()