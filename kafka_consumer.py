import os

from confluent_kafka import Consumer

from kafka.avro_factory import AvroFactory
from kafka.config.kakfka_config import KafkaConfig
from kafka.log_mangment import logger


class KafkaConsumer(KafkaConfig):
    """A Kafka consumer for consuming messages from a Kafka topic.

    This class provides methods to consume messages from a specified Kafka topic.
    It uses the `confluent_kafka` library for interacting with Kafka.

    Args:
        KafkaConfig (class): A subclass of `KafkaConfig` that provides Kafka configuration.


    Attributes:
        _msg: The last consumed message.
        _consumer: The Kafka consumer instance.

    """

    def __init__(self):
        """Initialize the KafkaConsumer object."""
        self._msg = None
        self._consumer = Consumer(KafkaConsumer._kafka_config_producer())

    def __callback_message(self):
        """Private method to retrieve the value of the last consumed message."""
        return self._msg.value()

    def commit(self) -> None:
        """ Commit the current message offset.

        This method commits the offset of the current consumed message and logs an
        info message indicating the commit.

        """
        self._consumer.commit(self._msg)
        logger.info(f"#{os.getpid()} - Worker commit - send data to consume_message")

    def consume_message(self, topic: str, serializer: bool = True, file_schema_raw: str = None) -> object | bytes:
        """ Consume messages from the specified Kafka topic.

        This method consumes messages from the specified Kafka topic and yields the
        deserialized message or the raw message value.

        Args:
            topic (str): The name of the Kafka topic to consume from.
            serializer (bool, optional): Flag indicating whether to deserialize the
                message using AvroFactory. Defaults to True.
            file_schema_raw (str, optional): The raw schema for deserialization. Only
                applicable when serializer is True. Defaults to None.

        Yields:
            object | bytes: The deserialized message object if serializer is True,
                otherwise the raw message value.

        Raises:
            Exception: If an error occurs while consuming messages, an exception is raised.

        Example:
            consumer = KafkaConsumer()
            topic = 'my_topic'
            for message in consumer.consume_message(topic):
                print(message)
                consumer.commit()

        """
        self.__subscribe(topic)

        while True:
            try:
                self._msg = self._consumer.poll(60)

                if self._msg is None:
                    continue

                if self._msg.error():
                    logger.error(f"#{os.getpid()} - Consumer error: {self._msg.error()}")
                    continue

                logger.info(f"#{os.getpid()} - Worker start - send data to consume_message")

                if serializer or file_schema_raw is not None:
                    yield AvroFactory.create_deserializer(topic, self._msg, file_schema_raw)
                else:
                    yield self.__callback_message()

            except Exception as e:
                logger.exception(f'tagger #{os.getpid()} - Worker terminated {e}')
                self._consumer.close()
                exit(1)

    def __subscribe(self, topic: str) -> list:
        """Subscribe to the specified Kafka topic.

        This method subscribes the consumer to the specified Kafka topic.

        Args:
            topic (str): The name of the Kafka topic to subscribe to.

        Returns:
            list: The list of topics subscribed to.

        """
        return self._consumer.subscribe(topics=[topic])
