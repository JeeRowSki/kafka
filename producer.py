import logging
import time
import uuid

from confluent_kafka import Producer
from message import Message

logger = logging.getLogger(__name__)


class MessageProducer:

    def __init__(self, bootstrap_servers, topic, producer_id):
        self.topic = topic
        self.producer_id = producer_id

        self.producer = Producer({
            "bootstrap.servers": bootstrap_servers,

            # Подтверждение от всех синхронизированных реплик
            "acks": "all",

            # Повторные попытки при временных ошибках
            "retries": 10,

            # Исключение дублирования при повторных отправках
            "enable.idempotence": True,

            # Максимальное время доставки сообщения
            "delivery.timeout.ms": 120000,

            # Небольшая задержка для группировки сообщений
            "linger.ms": 10,

            "client.id": producer_id,
        })

    def delivery_callback(self, err, msg):
        if err is not None:
            logger.error(
                "Delivery failed: %s",
                err
            )
        else:
            logger.info(
                "Delivered topic=%s partition=%s offset=%s",
                msg.topic(),
                msg.partition(),
                msg.offset()
            )

    def run(self):
        counter = 0

        try:
            while True:
                counter += 1

                message = Message.create(
                    message_id=str(uuid.uuid4()),
                    producer_id=self.producer_id,
                    text=f"Message number {counter}"
                )

                try:
                    serialized = message.serialize()

                    logger.info(
                        "SEND %s",
                        message
                    )

                    self.producer.produce(
                        topic=self.topic,
                        key=message.message_id.encode("utf-8"),
                        value=serialized,
                        callback=self.delivery_callback
                    )

                    # Обрабатывает события доставки и callbacks
                    self.producer.poll(0)

                except BufferError:
                    logger.warning(
                        "Producer queue is full"
                    )
                    self.producer.poll(1)

                except Exception:
                    logger.exception(
                        "Producer error"
                    )

                time.sleep(0.2)

        finally:
            # Ожидаем завершения отправки сообщений
            remaining = self.producer.flush(30)
            if remaining:
                logger.error(
                    "Producer stopped with %s undelivered messages",
                    remaining
                )
