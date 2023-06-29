import io
import json

import avro
import fastavro
from avro import schema
from avro.io import DatumWriter
from avro.schema import Schema
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer

from kafka.config.config import Config
from kafka.config.kakfka_config import KafkaConfig
from kafka.log_mangment import logger


class KafkaAvro(KafkaConfig):
    """
    KafkaAvro is a class for working with Avro serialization and deserialization in Kafka.

    This class provides methods for getting Avro serializers and deserializers, as well as
    writing Avro messages to a file.

    Args:
        KafkaConfig (class): A subclass of KafkaConfig used for Kafka configuration.

    Attributes:
        __message (dict): The Avro message to be serialized.
        __topic (str): The Kafka topic to produce or consume messages from.
        __file_schema_raw (str, optional): The raw schema for Avro serialization.
            Defaults to None.

    """

    def __init__(self, topic: str, message: dict, file_schema_raw: str = None):
        """ Initialize the KafkaAvro object.

        This method initializes the KafkaAvro object by setting the topic, message,
        and file_schema_raw attributes.

        Args:
            topic (str): The Kafka topic to produce or consume messages from.
            message (dict): The Avro message to be serialized.
            file_schema_raw (str, optional): The raw schema for Avro serialization.
                Defaults to None.

        """
        self.__message = message
        self.__topic = topic
        self.__file_schema_raw = file_schema_raw
        if file_schema_raw is None:
            self.__schema_registry_client = self.__schema_registry_client()
            self.__last_version = self.__get_last_version()

    def __get_last_version(self) -> str:
        """ Get the last version of the Avro schema from the schema registry.

        Returns:
            str: The last version of the Avro schema.

        """
        return self.__schema_registry_client.get_latest_version(f'{self.__topic}-value').schema.schema_str

    def __validate_message(self) -> None:
        """ Validate the Avro message.

        This method validates the Avro message against the Avro schema.

        Raises:
            ValueError: If the message is invalid.

        """
        try:
            _schema = fastavro.parse_schema(json.loads(self.__last_version))
            fastavro.schemaless_writer(io.BytesIO(), _schema, self.__message, strict=True)
        except ValueError as e:
            logger.error(f"Invalid message {e}")
            raise ValueError(f"Invalid message: {e}")

    def get_avro_serializer(self) -> AvroSerializer:
        """ Get the Avro serializer.

        Returns:
            AvroSerializer: An instance of the AvroSerializer class.

        """
        self.__validate_message()
        return AvroSerializer(self.__schema_registry_client,
                              self.__last_version,
                              conf={'auto.register.schemas': False, 'use.latest.version': True})

    def get_avro_deserializer(self) -> AvroDeserializer:
        """ Get the Avro deserializer.

        Returns:
            AvroDeserializer: An instance of the AvroDeserializer class.

        """
        return AvroDeserializer(self.__schema_registry_client, self.__last_version)

    def get_avro_serializer_file(self) -> bytes:
        """ Serialize the Avro message and write it to a file.

        Returns:
            bytes: The Avro serialized message as bytes.

        Raises:
            ValueError: If there is an error writing to the file.

        """
        bytes_writer = io.BytesIO()
        encoder = avro.io.BinaryEncoder(bytes_writer)
        try:
            datum_writer = DatumWriter(self._open_schema_registry())
            datum_writer.write(self.__message, encoder)
        except Exception as e:
            logger.error(f"Error writing to file {e}")
            raise ValueError(f"Error writing to file: {e}")
        return bytes_writer.getvalue()

    def _open_schema_registry(self) -> Schema:
        """ Open the Avro schema registry file.

        Returns:
            Schema: The Avro schema as an instance of the Schema class.

        Raises:
            ValueError: If the schema file is not found or there is an error reading the file.

        """
        avro_schema_file = str(Config.BASEPATH) + f"/file_avro/{self.__file_schema_raw}"
        try:
            with open(avro_schema_file, "r") as file:
                contents = schema.parse(file.read())
            return contents
        except FileNotFoundError:
            logger.error("File not found.")
            raise ValueError("File not found.")
        except IOError:
            logger.error("Error reading the file.")
            raise ValueError("Error reading the file.")

    @staticmethod
    def __schema_registry_client() -> SchemaRegistryClient:
        """ Get the SchemaRegistryClient instance.

        Returns:
            SchemaRegistryClient: An instance of the SchemaRegistryClient class.

        """
        return SchemaRegistryClient(KafkaAvro._schema_registry_config())
