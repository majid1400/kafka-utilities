from kafka.config.config import Config


class KafkaConfig:
    @staticmethod
    def _kafka_config() -> dict:
        return {
            'bootstrap.servers': Config.BOOTSTRAP_SERVERS,
            'security.protocol': 'SASL_SSL',
            'sasl.mechanisms': "SCRAM-SHA-512",
            'sasl.username': Config.SASL_USERNAME,
            'sasl.password': Config.SASL_PASSWORD,
        }

    @staticmethod
    def _kafka_config_producer() -> dict:
        list_config = {
            'group.id': Config.GROUP_ID,
            'auto.offset.reset': Config.AUTO_OFFSET_RESET,
            'enable.auto.commit': False,
        }
        _result = KafkaConfig._kafka_config()
        _result.update(list_config)
        return _result

    @staticmethod
    def _schema_registry_config() -> dict:
        return {'url': Config.SCHEMA_REGISTRY_URL,
                'ssl.ca.location': str(Config.BASEPATH) + '/certificate/CARoot.pem',
                'ssl.key.location': str(Config.BASEPATH) + '/certificate/key.pem',
                'ssl.certificate.location': str(Config.BASEPATH) + '/certificate/certificate.pem',
                'basic.auth.user.info': f'{Config.SCHEMA_REGISTRY_USERNAME}:'
                                        f'{Config.SCHEMA_REGISTRY_PASSWORD}',
                }
