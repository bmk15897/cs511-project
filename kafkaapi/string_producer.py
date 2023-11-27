from kafka import KafkaProducer


def produce_messages(broker, topic, num_messages):
    producer = KafkaProducer(bootstrap_servers=[broker])
    for i in range(num_messages):
        message = f"Produced Message {i}"
        producer.send(topic, message.encode('utf-8'))
    producer.flush()
    producer.close()

