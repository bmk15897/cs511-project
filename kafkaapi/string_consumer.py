from kafka import KafkaConsumer


def consume_messages(broker, topic, num_messages):
    consumer = KafkaConsumer(
        bootstrap_servers=broker,
        auto_offset_reset='earliest'
    )
    # consumer = KafkaConsumer(topic, bootstrap_servers=[broker], auto_offset_reset='earliest')
    consumer.subscribe(topics=topic)

    message_count = 0
    # raw_msgs = consumer.poll(timeout_ms=5000)
    for message in consumer:
        message_count += 1
        if message_count >= num_messages:
            break
    consumer.close()