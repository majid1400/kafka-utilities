from kafka.kafka_consumer import KafkaConsumer
from kafka.kafka_producer import KafkaProducer


class Kafka:
    """ Kafka is a utility class for producing and consuming messages to/from Kafka.

    This class provides static methods for producing and consuming messages to/from Kafka topics.

    Example:
        # Produce a message to a Kafka topic
        Kafka.produce_message(topic='my_topic', message={'key': 'value'})

        # Create a Kafka consumer
        consumer = Kafka.consumer()
        for message in consumer.consume_message(topic='my_topic'):
            print(message)
            consumer.commit()
    """

    @staticmethod
    def produce_message(topic: str, message: dict, serializer: bool = True, file_schema_raw: str = None) -> None:
        """ Produce a message to the specified Kafka topic.

        This method delegates the message production to the KafkaProducer class.

        Args:
            topic (str): The name of the Kafka topic to produce the message to.
            message (dict): The message to be produced.
            serializer (bool, optional): Flag indicating whether to serialize the message using AvroFactory.
                Defaults to True.
            file_schema_raw (str, optional): The raw schema for Avro serialization.
                Defaults to None.

        Example:
            Kafka.produce_message('my_topic', {'key': 'value'}, serializer=True)


        """
        return KafkaProducer().produce_message(topic, message, serializer, file_schema_raw)

    @staticmethod
    def consumer() -> KafkaConsumer:
        """ Create a KafkaConsumer instance.

        This method creates a KafkaConsumer instance for consuming messages from Kafka topics.

        Returns:
            KafkaConsumer: A KafkaConsumer instance.

        Example:
            consumer = Kafka.consumer()
            for message in consumer.consume_message('my_topic'):
                print(message)
                consumer.commit()

        """
        return KafkaConsumer()
