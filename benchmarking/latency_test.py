import kafkaapi
import time
from kafka import KafkaAdminClient

kafka_broker = 'localhost:9091'
kafka_topic = "kafka-test-topic"
kafka_num_messages = 5
red_panda_broker = 'localhost:9092'
red_panda_topic = "red-panda-test-topic"
red_panda_num_messages = 5

kafka_admin_client = KafkaAdminClient(
    bootstrap_servers=kafka_broker,
    client_id='test_client-kafka'
)

red_panda_admin_client = KafkaAdminClient(
    bootstrap_servers=red_panda_broker,
    client_id='test_client-red-panda'
)


def test_throughput_for_batch(kafka_or_redpanda, admin_client, broker, topic, num_messages):
    start_time = time.time()
    if kafka_or_redpanda == "kafka":
        kafkaapi.produce_messages(broker, topic, num_messages)
    else:
        kafkaapi.produce_messages(broker, topic, num_messages)
    end_time = time.time()
    duration = end_time - start_time
    throughput = num_messages / duration
    print(f"Produced {num_messages} messages in {duration} seconds. Throughput: {throughput} messages/sec.")

    start_time = time.time()
    if kafka_or_redpanda == "kafka":
        kafkaapi.consume_messages(broker, topic, num_messages)
    else:
        kafkaapi.consume_messages(broker, topic, num_messages)
    end_time = time.time()
    duration = end_time - start_time
    throughput = num_messages / duration
    print(f"Consumed {num_messages} messages in {duration} seconds. Throughput: {throughput} messages/sec.")
    try:
        admin_client.delete_topics([topic])
        print(f"Topic {topic} deleted successfully.")
    except Exception as e:
        print(f"An error occurred: {e}")


def test_throughput():
    print("Kafka Batch Throughput Test")
    test_throughput_for_batch("kafka", kafka_admin_client, kafka_broker, kafka_topic, kafka_num_messages)

    print()

    print("Red Panda Batch Throughput Test")
    test_throughput_for_batch("red-panda", red_panda_admin_client, red_panda_broker, red_panda_topic,
                              red_panda_num_messages)


def test_latency():
    duration = 0
    for i in range(kafka_num_messages):
        kafkaapi.produce_messages(kafka_broker, kafka_topic, 1)
        start_time = time.time()
        kafkaapi.consume_messages(kafka_broker, kafka_topic, 1)
        end_time = time.time()
        duration += (end_time - start_time)
    print(f"Average Latency for a Kafka message is {duration / kafka_num_messages}")

    try:
        kafka_admin_client.delete_topics([kafka_topic])
        print(f"Topic {kafka_topic} deleted successfully.")
    except Exception as e:
        print(f"An error occurred: {e}")
    # finally:
    # kafka_admin_client.close()
    print()

    duration = 0
    for i in range(red_panda_num_messages):
        kafkaapi.produce_messages(red_panda_broker, red_panda_topic, 1)
        start_time = time.time()
        kafkaapi.consume_messages(red_panda_broker, red_panda_topic, 1)
        end_time = time.time()
        duration += (end_time - start_time)
    print(f"Average Latency for a Red Panda message is {duration / red_panda_num_messages}")

    try:
        red_panda_admin_client.delete_topics([red_panda_topic])
        print(f"Topic {red_panda_topic} deleted successfully.")
    except Exception as e:
        print(f"An error occurred: {e}")
    # finally:
    # red_panda_admin_client.close()


test_throughput()
test_latency()
kafka_admin_client.close()
red_panda_admin_client.close()