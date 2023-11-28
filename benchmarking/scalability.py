import threading
import time
import matplotlib.pyplot as plt
from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient
import numpy as np

kafka_broker = 'localhost:9091'
red_panda_broker = 'localhost:9092'
topic = 'scalability-test-1'
num_producers = 1
num_consumers = 1
MESSAGE_SIZE = 1024  # 1 KB per message
TARGET_THROUGHPUT = 5 * 1024 * 1024  # 50 Mbps in bits
DURATION = 1  # Duration of the test in seconds

kafka_admin_client = KafkaAdminClient(
    bootstrap_servers=kafka_broker,
    client_id='test_client-kafka'
)

red_panda_admin_client = KafkaAdminClient(
    bootstrap_servers=red_panda_broker,
    client_id='test_client-red-panda'
)

def produce_messages(broker, topic, producer_id, metrics):
    producer = KafkaProducer(bootstrap_servers=[broker])
    message = 'x' * MESSAGE_SIZE
    end_time = start_time = time.time()
    message_count = 0

    while end_time - start_time < DURATION:
        send_time = time.time()
        producer.send(topic, f"{producer_id},{message_count},{send_time}".encode('utf-8'))
        message_count += 1
        # Adjust the sleep time to regulate the throughput
        time.sleep(max(0, (message_count / (TARGET_THROUGHPUT / 8 / MESSAGE_SIZE)) - (time.time() - start_time)))
        end_time = time.time()

    producer.flush()
    metrics['producers'][producer_id] = (end_time - start_time)/message_count
    producer.close()


def consume_messages(broker, topic, consumer_id, metrics):
    consumer = KafkaConsumer(topic, bootstrap_servers=[broker], auto_offset_reset='earliest')
    latencies = []
    start_time = time.time()

    while time.time() - start_time < DURATION:
        message = next(consumer)
        receive_time = time.time()
        _, _, sent_timestamp = message.value.decode('utf-8').split(',')
        latency = receive_time - float(sent_timestamp)
        latencies.append(latency)

    metrics['consumers'][consumer_id] = {
        'average_latency': np.mean(latencies),
        '95th_percentile_latency': np.percentile(latencies, 95),
        '99th_percentile_latency': np.percentile(latencies, 99)
    }
    consumer.close()


def run_scalability_test(broker, topic, num_producers, num_consumers):
    threads = []
    metrics = {'producers': {}, 'consumers': {}}

    # Start producer threads
    for i in range(num_producers):
        producer_thread = threading.Thread(target=produce_messages, args=(broker, topic, i, metrics))
        threads.append(producer_thread)
        producer_thread.start()

    # Start consumer threads
    for i in range(num_consumers):
        consumer_thread = threading.Thread(target=consume_messages, args=(broker, topic, i, metrics))
        threads.append(consumer_thread)
        consumer_thread.start()

    # Wait for all threads to complete
    for thread in threads:
        thread.join()

    return metrics


def plot_latency_comparison(kafka_metrics, red_panda_metrics, title):
    # Extracting Kafka metrics
    kafka_producer_avg_latency = np.mean(list(kafka_metrics['producers'].values()))
    kafka_consumer_avg_latency = np.mean([m['average_latency'] for m in kafka_metrics['consumers'].values()])
    kafka_consumer_95th_latency = np.mean([m['95th_percentile_latency'] for m in kafka_metrics['consumers'].values()])
    kafka_consumer_99th_latency = np.mean([m['99th_percentile_latency'] for m in kafka_metrics['consumers'].values()])

    # Extracting Red Panda metrics
    red_panda_producer_avg_latency = np.mean(list(red_panda_metrics['producers'].values()))
    red_panda_consumer_avg_latency = np.mean([m['average_latency'] for m in red_panda_metrics['consumers'].values()])
    red_panda_consumer_95th_latency = np.mean([m['95th_percentile_latency'] for m in red_panda_metrics['consumers'].values()])
    red_panda_consumer_99th_latency = np.mean([m['99th_percentile_latency'] for m in red_panda_metrics['consumers'].values()])

    # Labels
    labels = ['Producer Avg Latency', 'Consumer Avg Latency', 'Consumer p95.00', 'Consumer p99.00']

    # Kafka Metrics
    kafka_metrics = [kafka_producer_avg_latency, kafka_consumer_avg_latency, kafka_consumer_95th_latency, kafka_consumer_99th_latency]

    # Red Panda Metrics
    red_panda_metrics = [red_panda_producer_avg_latency, red_panda_consumer_avg_latency, red_panda_consumer_95th_latency, red_panda_consumer_99th_latency]

    x = np.arange(len(labels))  # the label locations
    width = 0.35  # the width of the bars

    fig, ax = plt.subplots()
    rects1 = ax.bar(x - width/2, kafka_metrics, width, label='Kafka')
    rects2 = ax.bar(x + width/2, red_panda_metrics, width, label='Red Panda')

    ax.set_ylabel('Latency (seconds)')
    ax.set_title(title)
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.legend()

    plt.tight_layout()
    plt.show()

print(time.time())
red_panda_metrics = run_scalability_test(red_panda_broker, topic, num_producers, num_consumers)
print(time.time())
kafka_metrics = run_scalability_test(kafka_broker, topic, num_producers, num_consumers)
print(time.time())
print(kafka_metrics)
print()
print(red_panda_metrics)
print()
plot_latency_comparison(kafka_metrics, red_panda_metrics, f"Kafka vs Red Panda Latency Comparison for {num_producers} P and {num_consumers} C")
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