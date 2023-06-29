# Project - Kafka Utilities

This repository contains utility classes for working with Kafka topics. The provided code snippets demonstrate how to
produce and consume messages from Kafka using various classes.

## Installation

Clone the repository and install the required dependencies:

```bash
git clone https://git.kavosh.org/python_common/python-client.git
cd kafka
pip install -r requirements.txt
```

## Code Overview

The repository contains the following utility classes:

1. `KafkaProducer`: A class for producing messages to Kafka topics.
2. `KafkaConsumer`: A class for consuming messages from Kafka topics.
3. `AvroFactory`: A factory class for creating Avro serializers and deserializers.
4. `Kafka`: A utility class that provides static methods for producing and consuming messages from Kafka topics, using
   the `KafkaProducer` and `KafkaConsumer` classes.
5. `KafkaAvro`: A class for handling Avro serialization and deserialization for Kafka messages.

## Usage

### Kafka

The `Kafka` class is a utility class that provides static methods for producing and consuming messages from Kafka
topics. It encapsulates the functionality of the `KafkaProducer` and `KafkaConsumer` classes.

```python
from kafka.kafka_main import Kafka

# Produce a message to a Kafka topic
Kafka.produce_message(topic='my_topic', message={'key': 'value'}, serializer=True)

# Create a Kafka consumer
consumer = Kafka.consumer()
for message in consumer.consume_message(topic='my_topic'):
    print(message)
    consumer.commit()

```

## Environment Configuration

Before running the code, make sure to set the necessary environment variables. Create a file named `.env` and set the
environment variables in the following format:

- `CONFIG_DEBUG`: Set to `1` to enable debug mode. Defaults to `0`.
- `CONFIG_ENV`: Set the environment (e.g., "production").
- `CONFIG_KAFKA_GROUP_ID`: Kafka consumer group ID.
- `CONFIG_KAFKA_AUTO_OFFSET_RESET`: Kafka consumer offset reset behavior (e.g., "earliest").
- `CONFIG_KAFKA_BOOTSTRAP_SERVERS`: Kafka bootstrap servers.
- `CONFIG_KAFKA_AUTO_SASL_USERNAME`: Kafka SASL username.
- `CONFIG_KAFKA_AUTO_SASL_PASSWORD`: Kafka SASL password.
- `CONFIG_KAFKA_SCHEMA_REGISTRY_URL`: Kafka Schema Registry URL.
- `CONFIG_KAFKA_SCHEMA_REGISTRY_USERNAME`: Kafka Schema Registry username.
- `CONFIG_KAFKA_SCHEMA_REGISTRY_PASSWORD`: Kafka Schema Registry password.

Make sure to set these environment variables before running the code snippets.

make a file named `.env` and here set the environment variable.
The list of environment variables is available in the config file.

## Contact

For any questions or inquiries, please contact Majid BehnamFard at [m.b1400v5@gmail.com](mailto:m.b1400v5@gmail.com).
