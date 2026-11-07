import logging

from confluent_kafka import Consumer, KafkaException
from message import Message

logger = logging.getLogger(__name__)


class SingleMessageConsumer:

    def __init__(self, bootstrap_servers, topic, instance_id):
        self.topic = topic

        self.consumer = Consumer({
            "bootstrap.servers": bootstrap_servers,

            # Уникальная группа для SingleMessageConsumer
            "group.id": "single-consumer-group",

            # Автоматическая фиксация offset
            "enable.auto.commit": True,

            # Интервал автоматического commit
            "auto.commit.interval.ms": 5000,

            # Читать с начала при отсутствии сохраненного offset
            "auto.offset.reset": "earliest",

            "client.id": f"single-{instance_id}",
        })

    def run(self):
        self.consumer.subscribe([self.topic])

        try:
            while True:
                try:
                    # Получаем одно сообщение
                    msg = self.consumer.poll(1.0)

                    if msg is None:
                        continue

                    if msg.error():
                        raise KafkaException(msg.error())

                    message = Message.deserialize(msg.value())

                    logger.info(
                        "SINGLE RECEIVED partition=%s offset=%s message=%s",
                        msg.partition(),
                        msg.offset(),
                        message
                    )

                except Exception:
                    logger.exception(
                        "Single consumer processing error"
                    )

        finally:
            self.consumer.close()
