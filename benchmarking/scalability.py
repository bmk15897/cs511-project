import threading
import time
import matplotlib.pyplot as plt
from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient
import numpy as np

def produce_messages(broker, topic, num_messages, producer_id, metrics):
    producer = KafkaProducer(bootstrap_servers=[broker])
    start_time = time.time()

    for i in range(num_messages):
        message = f"{producer_id},{i},{time.time()}"
        producer.send(topic, message.encode('utf-8'))
    producer.flush()

    end_time = time.time()
    metrics['producers'][producer_id] = end_time - start_time
    producer.close()


def consume_messages(broker, topic, consumer_id, num_messages, metrics):
    consumer = KafkaConsumer(topic, bootstrap_servers=[broker], auto_offset_reset='earliest')
    messages_consumed = 0
    total_latency = 0
    start_time = time.time()
    for message in consumer:
        messages_consumed += 1
        #_, _, sent_timestamp = message.value.decode('utf-8').split(',')
        if messages_consumed >= num_messages:
            break
    total_latency += (time.time() - start_time)
    metrics['consumers'][consumer_id] = {
        'total_latency': total_latency,
        'average_latency': total_latency / messages_consumed if messages_consumed > 0 else 0,
        'messages_consumed': messages_consumed
    }
    consumer.close()


def run_scalability_test(broker, topic, num_producers, num_consumers, num_messages):
    threads = []
    metrics = {'producers': {}, 'consumers': {}}

    # Start producer threads
    for i in range(num_producers):
        producer_thread = threading.Thread(target=produce_messages, args=(broker, topic, num_messages, i, metrics))
        threads.append(producer_thread)
        producer_thread.start()

    # Start consumer threads
    for i in range(num_consumers):
        consumer_thread = threading.Thread(target=consume_messages, args=(broker, topic, i, num_messages, metrics))
        threads.append(consumer_thread)
        consumer_thread.start()

    # Wait for all threads to complete
    for thread in threads:
        thread.join()

    return metrics


# Example usage
kafka_broker = 'localhost:9091'
red_panda_broker = 'localhost:9092'
topic = 'scalability-test-1'
num_producers = 5
num_consumers = 5
num_messages = 50000

kafka_admin_client = KafkaAdminClient(
    bootstrap_servers=kafka_broker,
    client_id='test_client-kafka'
)

red_panda_admin_client = KafkaAdminClient(
    bootstrap_servers=red_panda_broker,
    client_id='test_client-red-panda'
)


def collect_metrics_for_comparison(broker, num_producers_consumers_range, num_messages_range):
    metrics = {
        'producer_latency': [],
        'average_consumer_latency': [],
        'total_consumer_latency': [],
        'labels': []  # Store labels for configurations
    }

    for num_producers_consumers in num_producers_consumers_range:
        for num_messages in num_messages_range:
            test_metrics = run_scalability_test(broker, topic, num_producers_consumers, num_producers_consumers,
                                                num_messages)

            # Calculate average producer latency
            total_producer_time = sum(test_metrics['producers'].values())
            avg_producer_latency = total_producer_time / len(test_metrics['producers'])
            metrics['producer_latency'].append(avg_producer_latency)

            # Calculate consumer latencies
            total_consumer_latency = sum([x['total_latency'] for x in test_metrics['consumers'].values()])
            avg_consumer_latency = total_consumer_latency / len(test_metrics['consumers'])
            metrics['average_consumer_latency'].append(avg_consumer_latency)
            metrics['total_consumer_latency'].append(total_consumer_latency)

            # Generate label for this configuration
            label = f"{num_producers_consumers} P/C, {num_messages} Msgs"
            metrics['labels'].append(label)

    return metrics


def plot_comparison(metrics, metric_name, title):
    x = range(len(metrics['labels']))
    plt.plot(x, metrics['kafka'], label='Kafka')
    plt.plot(x, metrics['red_panda'], label='Red Panda')
    plt.xlabel('Configuration (Producers/Consumers, Messages)')
    plt.ylabel(metric_name)
    plt.title(title)
    plt.xticks(x, metrics['labels'], rotation=45)
    plt.legend()
    plt.tight_layout()
    plt.show()


# Running the tests
num_producers_consumers_range = range(3,4)
num_messages_range = [1, 10, 100]

kafka_metrics = collect_metrics_for_comparison(kafka_broker, num_producers_consumers_range, num_messages_range)
red_panda_metrics = collect_metrics_for_comparison(red_panda_broker, num_producers_consumers_range, num_messages_range)

# Combine Kafka and Red Panda metrics for comparison
combined_metrics = {
    'kafka': kafka_metrics['producer_latency'],
    'red_panda': red_panda_metrics['producer_latency'],
    'labels': kafka_metrics['labels']
}
plot_comparison(combined_metrics, 'Latency (seconds)', 'Average Producer Latency Comparison')

combined_metrics = {
    'kafka': kafka_metrics['average_consumer_latency'],
    'red_panda': red_panda_metrics['average_consumer_latency'],
    'labels': kafka_metrics['labels']
}
plot_comparison(combined_metrics, 'Latency (seconds)', 'Average Consumer Latency Comparison')

combined_metrics = {
    'kafka': kafka_metrics['total_consumer_latency'],
    'red_panda': red_panda_metrics['total_consumer_latency'],
    'labels': kafka_metrics['labels']
}
plot_comparison(combined_metrics, 'Latency (seconds)', 'Total Consumer Latency Comparison')

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