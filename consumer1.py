import time
import socket
import os
from kafka import KafkaConsumer
import json

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
wait_for_kafka(bootstrap_servers)

topic_name = os.environ.get('TOPIC', 'Topic1')
group_id = os.environ.get('CONSUMER_GROUP', 'group1')

consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=bootstrap_servers.split(','),
    auto_offset_reset='earliest',
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    group_id=group_id
)

print(f"Consuming messages from {topic_name} with group {group_id}...")

for message in consumer:
    print(message.value)