import logging
from collections import defaultdict

from confluent_kafka import (
    Consumer,
    KafkaException,
    TopicPartition,
)

from message import Message

logger = logging.getLogger(__name__)


class BatchMessageConsumer:

    BATCH_SIZE = 10

    def __init__(self, bootstrap_servers, topic, instance_id):
        self.topic = topic
        self.batch = []

        self.consumer = Consumer({
            "bootstrap.servers": bootstrap_servers,

            # Отдельная группа консьюмеров
            "group.id": "batch-consumer-group",

            # Ручная фиксация offset
            "enable.auto.commit": False,

            # Не сохранять позицию автоматически
            "enable.auto.offset.store": False,

            # Читать с начала, если offset группы отсутствует
            "auto.offset.reset": "earliest",

            # Минимальный размер ответа брокера
            "fetch.min.bytes": 1024,

            # Максимальное ожидание ответа брокера
            "fetch.wait.max.ms": 1000,

            "client.id": f"batch-{instance_id}",
        })

    def process_batch(self):
        if not self.batch:
            return

        offsets = defaultdict(int)

        try:
            decoded = []

            for msg in self.batch:
                message = Message.deserialize(msg.value())
                decoded.append((msg, message))

            for msg, message in decoded:
                logger.info(
                    "BATCH RECEIVED partition=%s offset=%s message=%s",
                    msg.partition(),
                    msg.offset(),
                    message,
                )

                key = (msg.topic(), msg.partition())
                offsets[key] = max(
                    offsets[key],
                    msg.offset() + 1
                )

            partitions = [
                TopicPartition(topic, partition, offset)
                for (topic, partition), offset in offsets.items()
            ]

            result = self.consumer.commit(
                offsets=partitions,
                asynchronous=False
            )

            for partition in result:
                if partition.error is not None:
                    raise KafkaException(partition.error)

            logger.info(
                "BATCH COMMIT count=%s offsets=%s",
                len(self.batch),
                dict(offsets)
            )

            self.batch.clear()

        except Exception:
            logger.exception(
                "Batch processing failed; offsets not committed"
            )

            self.batch.clear()

            try:
                self.consumer.unsubscribe()
                self.consumer.subscribe([self.topic])
            except Exception:
                logger.exception("Consumer resubscribe failed")
                raise

    def run(self):
        self.consumer.subscribe([self.topic])

        try:
            while True:
                try:
                    messages = self.consumer.consume(
                        num_messages=self.BATCH_SIZE - len(self.batch),
                        timeout=1.0
                    )

                    for msg in messages:
                        if msg.error():
                            raise KafkaException(msg.error())

                        self.batch.append(msg)

                    if len(self.batch) >= self.BATCH_SIZE:
                        self.process_batch()

                except Exception:
                    logger.exception(
                        "Batch consumer polling error"
                    )

                    self.batch.clear()

                    try:
                        self.consumer.unsubscribe()
                        self.consumer.subscribe([self.topic])
                    except Exception:
                        logger.exception("Batch recovery failed")
                        raise

        finally:
            self.consumer.close()
