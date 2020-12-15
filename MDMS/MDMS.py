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
client_id = f'mdms-mqtt-{random.randint(0, 1000)}'


def connect_mqtt():
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)

    client = mqtt_client.Client(client_id)
    client.on_connect = on_connect
    client.connect(broker, port)
    return client


def publish_prix(client):
    msg_count = 0
    time.sleep(2)
    while(msg_count <3):
        msg = str({"id": client_id, "prix":4})
        result = client.publish(topic_1, msg)
        # result: [0, 1]
        status = result[0]
        if status == 0:
            print(f"Send `{msg}` to topic `{topic_1}`")
        else:
            print(f"Failed to send message to topic {topic_1}")
        msg_count += 1
        threading.Timer(60.0*60.0, publish_prix).start()
    
    
def publish_reduction(client):
    msg_count = 0
    time.sleep(1)
    while(msg_count <2):
        msg = str({"id": client_id, "reduction":2})
        result = client.publish(topic_2, msg)
        # result: [0, 1]
        status = result[0]
        if status == 0:
            print(f"Send `{msg}` to topic `{topic_2}`")
        else:
            print(f"Failed to send message to topic {topic_2}")
        msg_count += 1
        threading.Timer(60.0, publish_reduction).start()
    

def subscribe(client: mqtt_client):
    def on_message(client, userdata, msg):
        print(f"Received `{msg.payload.decode()}` from `{msg.topic}` topic")

    client.subscribe(topic_3)
    client.subscribe(topic_4)
    client.on_message = on_message

def run():
    client = connect_mqtt()
    client.loop_start()
    subscribe(client)
    publish_prix(client)
    publish_reduction(client)
    



if __name__ == '__main__':
    run()