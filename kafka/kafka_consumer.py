from kafka import KafkaConsumer
import os
from time import sleep
from json import dumps, loads
import json
from s3fs import S3FileSystem
import yfinance as yf

# Retrieve Kafka broker and topic information from environment variables
KAFKA_BROKER_URL = os.environ.get('KAFKA_BROKER_URL')
STOCK_PRICE_TOPIC = os.environ.get('INDIVIDUAL_STOCK_TOPIC')
SP500_TOPIC = os.environ.get('SP500_TOPIC')

s3 = S3FileSystem()

if __name__ == '__main__':
    stock_price_consumer = KafkaConsumer(
        STOCK_PRICE_TOPIC,
        bootstrap_servers=KAFKA_BROKER_URL,
        value_deserializer=lambda x: loads(x.decode('utf-8'))
    )

    sp500_price_consumer = KafkaConsumer(
        SP500_TOPIC,
        bootstrap_servers=KAFKA_BROKER_URL,
        value_deserializer=lambda x: loads(x.decode('utf-8'))
    )

    for message in stock_price_consumer:
        with s3.open("s3://realtime-stock-price/trade_price.json", 'w') as file:
            print(message)
            json.dump(message, file)

    for message in sp500_price_consumer:
        with s3.open("s3://sp500-real-time-price/trade_price.json", 'w') as file:
            print(message)
            json.dump(message, file) 
            