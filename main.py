import logging
import os
import socket
import threading
import time

from producer import MessageProducer
from single_consumer import SingleMessageConsumer
from batch_consumer import BatchMessageConsumer


logging.basicConfig(
    level=logging.INFO,
    format=(
        "%(asctime)s "
        "%(levelname)s "
        "[%(threadName)s] "
        "%(message)s"
    ),
)

logger = logging.getLogger(__name__)


def main():
    bootstrap_servers = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        "kafka-0:9092,kafka-1:9092,kafka-2:9092"
    )

    topic = os.getenv(
        "KAFKA_TOPIC",
        "practice2-topic"
    )

    instance_id = os.getenv(
        "HOSTNAME",
        socket.gethostname()
    )

    logger.info(
        "Starting application instance=%s topic=%s",
        instance_id,
        topic
    )

    producer = MessageProducer(
        bootstrap_servers,
        topic,
        instance_id
    )

    single_consumer = SingleMessageConsumer(
        bootstrap_servers,
        topic,
        instance_id
    )

    batch_consumer = BatchMessageConsumer(
        bootstrap_servers,
        topic,
        instance_id
    )

    threads = [
        threading.Thread(
            target=producer.run,
            name="Producer",
            daemon=True
        ),
        threading.Thread(
            target=single_consumer.run,
            name="SingleConsumer",
            daemon=True
        ),
        threading.Thread(
            target=batch_consumer.run,
            name="BatchConsumer",
            daemon=True
        ),
    ]

    for thread in threads:
        thread.start()

    try:
        while True:
            time.sleep(5)

            for thread in threads:
                if not thread.is_alive():
                    logger.error(
                        "Thread %s stopped unexpectedly",
                        thread.name
                    )
                    raise RuntimeError(
                        f"Thread {thread.name} stopped"
                    )

    except KeyboardInterrupt:
        logger.info("Stopping application")


if __name__ == "__main__":
    main()
