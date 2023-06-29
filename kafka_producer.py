import json

from confluent_kafka import Producer

from kafka.avro_factory import AvroFactory
from kafka.config.kakfka_config import KafkaConfig
from kafka.log_mangment import logger


class KafkaProducer(KafkaConfig):
    """ KafkaProducer is a class for producing messages to a Kafka topic.

    This class provides methods for producing messages to a specified Kafka topic.

    Args:
        KafkaConfig (class): A subclass of KafkaConfig used for Kafka configuration.

    Attributes:
        _producer: The Kafka producer instance.

    """
    def __init__(self):
        """ Initialize the KafkaProducer object.

        This method initializes the KafkaProducer object by creating a Kafka producer
        instance using the KafkaConfig and logs an info message.

        """
        self._producer = Producer(KafkaProducer._kafka_config())
        logger.info("Producing user records to topic.")

    def produce_message(self, topic: str, message: dict, serializer: bool, file_schema_raw: str = None) -> None:
        """ Produce a message to the specified Kafka topic.

         This method produces a message to the specified Kafka topic using the Kafka producer.
         The message can be serialized using AvroFactory or sent as a raw JSON message.

         Args:
             topic (str): The name of the Kafka topic to produce the message to.
             message (dict): The message to be produced.
             serializer (bool): Flag indicating whether to serialize the message using AvroFactory.
             file_schema_raw (str, optional): The raw schema for Avro serialization. Defaults to None.

         """

        if serializer or file_schema_raw is not None:
            __avro_serializer = AvroFactory.create_serializer(topic, message, file_schema_raw)
            self._producer.produce(topic=topic, value=__avro_serializer, on_delivery=self.__delivery_report)
        else:
            self._producer.produce(topic=topic, value=json.dumps(message), on_delivery=self.__delivery_report)
        self._producer.flush()

    @staticmethod
    def __delivery_report(err, _message) -> None:
        """ Delivery report callback function.

         This method is a callback function that is called by the Kafka producer to
         report the delivery status of a produced message.

         Args:
             err (KafkaError): The error object, if the delivery failed.
             _message (Message): The produced message.

         """
        if err is not None:
            logger.error(f"Delivery failed for User record {_message.key()}: {err}")
            return
        logger.info(f"*** successfully produced to {_message.topic()}"
                    f" [{_message.partition()}] at offset {_message.offset()} ***")
