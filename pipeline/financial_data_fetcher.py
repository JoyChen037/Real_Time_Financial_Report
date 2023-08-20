import sys
sys.path.append("C:/Users/joych/OneDrive/Desktop/trading/")
import data
import requests
from datetime import datetime
import pandas as pd
import bs4 as bs
from bs4 import BeautifulSoup
import yfinance as yf
from typing import List, Dict

# Function to extract S&P500 company information from Wikipedia
def get_sp500_comp_list():
    """
    Scrapes Wikipedia to extract S&P500 company information and stores it in a DataFrame.
    The DataFrame is then uploaded to an S3 bucket.

    Dependencies:
        - pandas
        - bs4 (BeautifulSoup)
        - requests
        - data (contains dataframe_to_s3 function)

    Returns:
        None
    """
    tickers = []
    companies = []
    sectors = []
    sub_industries = []

    timeout_seconds = 10
    try:
        resp = requests.get('http://en.wikipedia.org/wiki/List_of_S%26P_500_companies', 
                                timeout=timeout_seconds)
        # Process the response
        soup = bs.BeautifulSoup(resp.text, 'lxml')
        table = soup.find('table', {'class': 'wikitable sortable'})
        for row in table.findAll('tr')[1:]:
            tickers.append(row.findAll('td')[0].text.split('\n')[0])
            companies.append(row.findAll('td')[1].text.split('\n')[0])
            sectors.append(row.findAll('td')[2].text.split('\n')[0])
            sub_industries.append(row.findAll('td')[3].text.split('\n')[0])
        
        sp500 = pd.DataFrame({
            'ticker': tickers,
            'company': companies,
            'sector': sectors,
            'sub_industry': sub_industries
        })
        print("exeucted!")
        data.dataframe_to_s3(data.s3_client, sp500, 
                            'airflow-stock-tickers-on-us-exchanges', 'ticker', 'parquet')
    except requests.Timeout:
        print("The request timed out.")
    except requests.RequestException as e:
        print(f"An error occurred: {e}")

# Function to extract historical stock prices for S&P500 companies
def get_sp_historical_price():
    """
    Downloads historical stock prices for S&P500 companies from Yahoo Finance API.
    The data is then transformed and uploaded to an S3 bucket.

    Dependencies:
        - pandas
        - yfinance
        - data (contains read_from_s3 and dataframe_to_s3 functions)

    Returns:
        None
    """
    sp500_comp_df = data.read_from_s3(data.s3_client,
                               'airflow-stock-tickers-on-us-exchanges', 'ticker', 'parquet')
    tickers = sp500_comp_df['ticker'].tolist()
    ticker_search_str = ' '.join(tickers)

    raw_data = yf.download(tickers=ticker_search_str, period='60d', interval="1d")
    raw_data = raw_data.stack().reset_index().rename({'level_0': 'DateTime', 
                                                      'level_1': 'Ticker'}, axis=1)

    data.dataframe_to_s3(data.s3_client, raw_data, 'sp500stockprice', 'sp_hist_rice', 'parquet')

# Function to extract historical stock prices for sector ETFs
def get_sector_etf_historical_price():
    """
    Downloads historical sector ETFs' prices (XLE, XLB...) from Yahoo Finance API.
    The data is then transformed and uploaded to an S3 bucket.

    Dependencies:
        - pandas
        - yfinance
        - data (contains read_from_s3 and dataframe_to_s3 functions)

    Returns:
        None
    """
    raw_data = yf.download(
        tickers="SPY XLB XLC XLE XLF XLI XLK XLP XLRE XLU XLV XLY",
        period='60d',
        interval="1d"
    )
    raw_data = raw_data.stack().reset_index().rename(
        {'level_0': 'DateTime', 'level_1': 'Ticker'},
        axis=1
    )
    data.dataframe_to_s3(
        data.s3_client,
        raw_data,
        'sectorprice',
        'sector_ETF_price',
        'parquet'
    )

def get_intraday_stock_price(ticker_search_str: str) -> List[Dict[str, str]]:
    """
    Fetches intraday trading prices of S&P 500 individual stocks.

    Args:
        ticker_search_str (str): A string containing tickers of S&P 500 companies separated by spaces.

    Returns:
        List[Dict[str, str]]: A list of dictionaries, each representing a stock's trading price data.
            Each dictionary has the following keys:
                - 'DateTime': Timestamp of the trading data
                - 'Ticker': Ticker symbol of the stock
                - 'Open': Opening price
                - 'High': Highest price during the interval
                - 'Low': Lowest price during the interval
                - 'Close': Closing price
                - 'Volume': Volume of trading
    """
    # Fetch intraday trading price data
    stocks_data = yf.download(tickers=ticker_search_str, period='1d', interval='30m')
    stocks_data = stocks_data.stack().reset_index().rename({'level_0': 'DateTime', 
                                                            'level_1': 'Ticker'}, axis=1)
    stocks_data['Datetime'] = stocks_data['Datetime'].astype(str)
    stock_price_dict = stocks_data.to_dict(orient="records")
    return stock_price_dict

def get_intraday_sp500_price() -> List[Dict[str, str]]:
    """
    Fetches intraday trading prices of the S&P 500 index.

    Returns:
        List[Dict[str, str]]: A list of dictionaries, each representing trading price data for the S&P 500 index.
            Each dictionary has the following keys:
                - 'DateTime': Timestamp of the trading data
                - 'Ticker': Ticker symbol of the index (usually '^GSPC' for the S&P 500)
                - 'Open': Opening price
                - 'High': Highest price during the interval
                - 'Low': Lowest price during the interval
                - 'Close': Closing price
                - 'Volume': Volume of trading
    """
    # Fetch intraday trading price data for S&P 500 index
    sp500_data = yf.download(tickers="^GSPC", period='1d', interval='30m')
    sp500_data = sp500_data.stack().reset_index().rename({'level_0': 'DateTime',
                                                          'level_1': 'Ticker'}, axis=1)
    sp500_data['Datetime'] = sp500_data['Datetime'].astype(str)
    sp500_price_dict = sp500_data.to_dict(orient="records")
    return sp500_price_dict

def run_news_etl():
    """
    Scrapes news of S&P 500 companies from financialmodelingprep.com and stores it in S3.

    Returns:
        pandas.DataFrame: DataFrame containing the scraped news data.
    """
    # Read S&P 500 tickers from S3
    comp_df = data.read_from_s3(data.s3_client, 'airflow-stock-tickers-on-us-exchanges', 'ticker', 'parquet')
    print(comp_df)
    
    # Extract tickers from DataFrame
    tickers = comp_df['ticker'].tolist()
    
    news = []
    for ticker in tickers:
        url = "https://financialmodelingprep.com/financial-summary/" + ticker
        request = requests.get(url)
        parser = BeautifulSoup(request.text, "html.parser")
        news_html = parser.find_all('a', {'class': 'article'})
        for i in range(0, len(news_html)):
            news.append(
                {
                    'ticker': ticker,
                    'link': news_html[i]['href'],
                    'date': news_html[i].find('h5', {'class': 'article__date'}).text,
                    'title': news_html[i].find('h4', {'class': 'article__title-text'}).text,
                    'text': news_html[i].find('p', {'class': 'article__text'}).text
                }
            )
    
    # Create DataFrame from the collected news
    news_df = pd.DataFrame(news)
    news_df = news_df.reset_index()
    news_df['date'] = [datetime.strptime(date, '%d %B %Y').strftime('%Y-%m-%d') for date in news_df['date']]
    
    # Store DataFrame in S3
    data.dataframe_to_s3(data.s3_client, news_df, 'sp500-stock-news', 'sp500news', 'parquet')
    
    return news_df


if __name__ == "__main__":
    pass

    # end of the file
