import random
import string

from kafka import KafkaProducer, KafkaAdminClient
import time
import json

from kafka.admin import NewTopic

broker = ['localhost:9091', 'localhost:9093', 'localhost:9094']
producer = KafkaProducer(
    bootstrap_servers=broker,
    acks='all',
    linger_ms=1,
    batch_size=131072,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)


def generate_payload(size):
    """Generate a random string of specified size."""
    return ''.join(random.choices(string.ascii_letters + string.digits, k=size))


admin_client = KafkaAdminClient(
    bootstrap_servers=broker,
    client_id='test_client-kafka'
)
topic = 't6'

MESSAGE_SIZE = 10000
for i in range(1001):
    payload = generate_payload(MESSAGE_SIZE)
    message = {
        'timestamp': int(round(time.time() * 1000)),  # current time in milliseconds
        'payload': payload
    }
    producer.send(topic, value=message)
    time.sleep(0.2)  # send a message every second
