# producer.py
import os
import pandas as pd
from kafka import KafkaProducer
import json
import time
import socket

def wait_for_kafka(servers, timeout=30):
    for server in servers.split(','):
        host, port = server.split(':')
        port = int(port)
        start = time.time()
        while True:
            try:
                s = socket.create_connection((host, port), timeout=1)
                s.close()
                break
            except:
                if time.time() - start > timeout:
                    raise TimeoutError(f"Cannot connect to Kafka broker {host}:{port}")
                time.sleep(1)

bootstrap_servers = os.environ.get('BOOTSTRAP_SERVERS', 'broker-1:29091,broker-2:29092')
topic1 = os.environ.get('TOPIC1', 'Topic1')
topic2 = os.environ.get('TOPIC2', 'Topic2')
topics = [topic1, topic2]

# Чекаємо, поки брокери запустяться
wait_for_kafka(bootstrap_servers)

# Зчитуємо CSV
df = pd.read_csv("data/trips.csv")

# Підключення до Kafka
producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers.split(','),
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print(f"Sending messages to topics: {topics}")

for index, row in df.iterrows():
    message = row.to_dict()
    for topic in topics:
        producer.send(topic, value=message)
        print(f"Sent to {topic}: {message}")
    time.sleep(1)

producer.flush()
producer.close()