from confluent_kafka import Consumer, KafkaError


def consume_messages():
    consumer_config = {
        'bootstrap.servers': 'localhost:29092',  # Адреса Kafka брокера
        'group.id': 'my-consumer-group',
        'auto.offset.reset': 'earliest'
    }
    consumer = Consumer(consumer_config)
    topic = 'my-topic'
    consumer.subscribe([topic])

    time_window = []  # Список для зберігання часових міток
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print(
                    f'End of partition reached: {msg.topic()} [{msg.partition()}]')
            else:
                print(f'Error while consuming message: {msg.error()}')
        else:
            timestamp_seconds = int(msg.value().decode("utf-8"))
            time_window.append(timestamp_seconds)
            # Виведення поточного часу в секундах у консоль
            print(f"Seconds: {timestamp_seconds}")
            if len(time_window) >= 10:
                average_timestamp = sum(time_window) / len(time_window)
                print(
                    f'Average timestamp over 10 seconds: {average_timestamp}')
                time_window = []  # Очистка списку для нового обрахунку


if __name__ == '__main__':
    consume_messages()
