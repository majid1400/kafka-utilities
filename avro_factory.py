from confluent_kafka import cimpl
from confluent_kafka.serialization import SerializationContext, MessageField

from kafka.kafka_avro import KafkaAvro


class AvroFactory:
    """ AvroFactory is a factory class for creating Avro serializers and deserializers.

    This class provides static methods for creating Avro serializers and deserializers
    based on the topic, message, and optional file schema.

    """
    @staticmethod
    def create_serializer(topic: str, message: dict, file_schema_raw: str = None) -> bytes:
        """ Create an Avro serializer.

        This method creates an Avro serializer based on the topic, message, and optional file schema.

        Args:
            topic (str): The Kafka topic associated with the message.
            message (dict): The message to be serialized.
            file_schema_raw (str, optional): The raw schema for Avro serialization.
                Defaults to None.

        Returns:
            bytes: The Avro serialized message.

        """
        if file_schema_raw is None:
            _avro = KafkaAvro(topic, message)
            avro_serializer = _avro.get_avro_serializer()
            return avro_serializer(message, SerializationContext(topic, MessageField.VALUE))
        else:
            _avro = KafkaAvro(topic, message, file_schema_raw)
            return _avro.get_avro_serializer_file()

    @staticmethod
    def create_deserializer(topic: str, msg: cimpl, file_schema_raw: str = None) -> object:
        """ Create an Avro deserializer.

        This method creates an Avro deserializer based on the topic, message, and optional file schema.

        Args:
            topic (str): The Kafka topic associated with the message.
            msg (cimpl): The Kafka message.
            file_schema_raw (str, optional): The raw schema for Avro serialization.
                Defaults to None.

        Returns:
            object: The deserialized message object.

        Raises:
            NotImplementedError: If file schema is provided (not supported yet).

        """
        if file_schema_raw is None:
            _avro = KafkaAvro(topic, msg)
            avro_deserializer = _avro.get_avro_deserializer()
            return avro_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
        else:
            # TODO: support file schema
            raise NotImplementedError
