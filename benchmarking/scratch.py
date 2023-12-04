import threading
import time
import matplotlib.pyplot as plt
from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient
from kafka.admin import NewTopic
import numpy as np

kafka_brokers = 'localhost:9092'
red_panda_brokers = 'localhost:9091'
topic = 'scalability-test-1'
num_producers = 1
num_consumers = 1
MESSAGE_SIZE = 1024  # 1 KB per message
TARGET_THROUGHPUT = 5 * 1024 * 1024  # 50 Mbps in bits
DURATION = 5  # Duration of the test in seconds

kafka_admin_client = KafkaAdminClient(
    bootstrap_servers=kafka_brokers,
    client_id='test_client-kafka'
)

red_panda_admin_client = KafkaAdminClient(
    bootstrap_servers=red_panda_brokers,
    client_id='test_client-red-panda'
)
try:
    kafka_admin_client.delete_topics([topic])
    print(f"Topic {topic} deleted successfully.")
except Exception as e:
    print(f"An error occurred: {e}")

try:
    red_panda_admin_client.delete_topics([topic])
    print(f"Topic {topic} deleted successfully.")
except Exception as e:
    print(f"An error occurred: {e}")