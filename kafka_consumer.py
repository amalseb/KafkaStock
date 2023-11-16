import json
from json import loads

from kafka import KafkaConsumer
from s3fs import S3FileSystem

import my_data


class CustomKafkaConsumer:
    def __init__(self):
        pass

    def get_consumer(self):
        consumer = KafkaConsumer(
            'demo_test',
            bootstrap_servers=[my_data.PUBLIC_IP+':9092'],          # REPLACE with public IP
            value_deserializer=lambda x: loads(x.decode('utf-8'))
        )
        return consumer

    def dump_s3(self, consumer):
        s3 = S3FileSystem()

        for count, item in enumerate(consumer):
            with s3.open("s3://kafka-stock-market-amalseb/stock_market_{}".format(count), 'w') as file:
                json.dump(item.value, file)

if __name__ == "__main__":
    cons = CustomKafkaConsumer()
    consumer = cons.get_consumer()
    cons.dump_s3(consumer)
