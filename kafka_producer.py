from json import dumps
from time import sleep

import pandas as pd
from kafka import KafkaProducer

import my_data


class CustomKafkaProducer:
    def __init__(self):
        pass

    def read_stock_data_from_url(self, url):
        stock_df = pd.read_csv(url)
        return stock_df

    def get_producer(self):
        producer = KafkaProducer(bootstrap_servers=[my_data.PUBLIC_IP+':9092'],     # REPLACE with public IP
                                 value_serializer=lambda x:
                                 dumps(x).encode('utf-8'))
        return producer

    def send_producer(self, producer, stock_df):
        while True:
            dict_stock = stock_df.sample(1).to_dict(orient="records")[0]
            producer.send('demo_test', value=dict_stock)
            sleep(1)


if __name__ == '__main__':
    prod = CustomKafkaProducer()
    url = ("https://raw.githubusercontent.com/darshilparmar/stock-market-kafka-data-engineering-project/main"
           "/indexProcessed.csv")
    stock_df = prod.read_stock_data_from_url(url)
    producer = prod.get_producer()
    prod.send_producer(producer, stock_df)
