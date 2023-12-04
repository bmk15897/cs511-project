from kafka import KafkaConsumer, KafkaAdminClient
import json
import time
import numpy as np
from kafka.admin import NewTopic

broker = ['localhost:9092', 'localhost:9093', 'localhost:9094']
admin_client = KafkaAdminClient(
    bootstrap_servers=broker,
    client_id='test_client-kafka'
)

topic = 't6'
admin_client.create_topics(new_topics=[NewTopic(name=topic, num_partitions=1, replication_factor=1)])

consumer = KafkaConsumer(
    topic,
    bootstrap_servers=broker,
    auto_offset_reset='earliest',
    enable_auto_commit=False,
    max_partition_fetch_bytes=131072,
    auto_commit_interval_ms=0,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)
latencies = []
cnt = 0
for message in consumer:
    received_time = int(round(time.time() * 1000))
    latency = received_time - message.value['timestamp']
    latencies.append(latency)
    cnt += 1
    if cnt == 1001:
        break

latencies = latencies[1:]
latencies.sort()
metrics = {
        'average_latency': np.mean(latencies),
        '51st_percentile_latency': np.percentile(latencies, 51),
        '80th_percentile_latency': np.percentile(latencies, 80),
        '90th_percentile_latency': np.percentile(latencies, 90),
        '95th_percentile_latency': np.percentile(latencies, 95),
        '99th_percentile_latency': np.percentile(latencies, 99)
    }

print(metrics)
