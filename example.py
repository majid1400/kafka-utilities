import time

from kafka.kafka_main import Kafka

if __name__ == '__main__':
    # ------------ consumer ------------ #
    # consumer = Kafka.consumer()
    # message = consumer.consume_message('raw-data-model')
    # for i in message:
    #     print(i)
    #     consumer.commit()

    # ------------ produce ------------ #
    msg = {"publish_ts": int(time.time()),
           "country": "iran",
           "platform": "twitter",
           "sub_platform": "post",
           "user_id": "123456789",
           "raw_data": "{'tes':'value'}",
           "fetch_ts": int(time.time()),
           "fetch_info": "hashtag crawler"}
    Kafka.produce_message('raw-data-model', msg)
