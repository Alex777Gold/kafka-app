import time
from confluent_kafka import Producer


def produce_messages():
    producer_config = {
        'bootstrap.servers': 'localhost:29092'  # Адреса Kafka брокера
    }
    producer = Producer(producer_config)
    topic = 'my-topic'

    while True:
        # Отримання поточного часу в секундах (Unix timestamp)
        timestamp_seconds = int(time.time())
        producer.produce(topic, str(timestamp_seconds))
        # Виведення поточного часу в секундах у консоль
        print(f"Seconds: {timestamp_seconds}")
        # Затримка 1 секунда перед надсиланням наступного повідомлення
        time.sleep(1)


if __name__ == '__main__':
    produce_messages()
