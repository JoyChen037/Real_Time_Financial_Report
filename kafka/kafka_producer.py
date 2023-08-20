import sys
sys.path.append("C:/Users/joych/OneDrive/Desktop/trading/")
import data
import financial_data_fetcher
import yfinance as yf
import pandas as pd
from kafka import KafkaConsumer, KafkaProducer
from time import sleep
from json import dumps
import os
import logging

# Configure logging
logging.basicConfig(level=logging.DEBUG)

# Retrieve Kafka broker and topic information from environment variables
KAFKA_BROKER_URL = os.environ.get('KAFKA_BROKER_URL')
STOCK_PRICE_TOPIC = os.environ.get('INDIVIDUAL_STOCK_TOPIC')
SP500_TOPIC = os.environ.get('SP500_TOPIC')

# Callback function for successful message sending
def on_send_success(record_metadata):
    """
    Callback function to handle successful message sending.

    Parameters:
        record_metadata (object): Metadata of the sent record.

    Returns:
        None
    """
    print("Message sent successfully")
    print("Topic:", record_metadata.topic)
    print("Partition:", record_metadata.partition)
    print("Offset:", record_metadata.offset)

# Callback function for message sending error
def on_send_error(excp):
    """
    Callback function to handle message sending errors.

    Parameters:
        excp (exception): Exception object representing the error.

    Returns:
        None
    """
    logging.error('An error occurred while sending the message', exc_info=excp)

if __name__ == '__main__':
    # Read S&P 500 tickers from S3
    sp500_comp_DF = data.read_from_s3(data.s3_client,
                                      'airflow-stock-tickers-on-us-exchanges', 'ticker', 'parquet')
    
    # Generate a string of tickers of S&P 500 companies
    ticker_search_str = ' '.join(sp500_comp_DF['ticker'].tolist())
    
    # Initialize Kafka producer
    producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER_URL,
                             value_serializer=lambda x: dumps(x).encode('utf-8'),
                             max_block_ms=100,
                             retries=1)

    while True:
        # S&P individual stock trading price
        stock_price_dict = financial_data_fetcher.get_intraday_stock_price(ticker_search_str)

        try:
            producer.send(STOCK_PRICE_TOPIC, value=stock_price_dict) \
                    .add_callback(on_send_success) \
                    .add_errback(on_send_error)
            producer.flush()
        except IOError as e:
            print("IO error")
        except ValueError:
            print("Conversion error")

        # S&P 500 index trading price
        sp500_price_dict = financial_data_fetcher.get_intraday_sp500_price()

        try:
            producer.send(SP500_TOPIC, value=sp500_price_dict) \
                    .add_callback(on_send_success) \
                    .add_errback(on_send_error)
            producer.flush()
        except IOError as e:
            print("IO error")
        except ValueError:
            print("Conversion error")
