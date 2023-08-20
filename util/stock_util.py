import yfinance as yf
import numpy as np
import streamlit as st
import pandas as pd

@st.cache_data
def calculate_daily_returns(dataframe):
    """
    Calculate daily returns for each sector in the given dataframe.

    Parameters:
    dataframe (pd.DataFrame): A pandas DataFrame containing 'Date', 'Ticker', 'Open', and 'Close' columns.
    
    Returns:
    pd.DataFrame: A new DataFrame containing the daily returns for each sector.
    """
    # Pivot the dataframe to have tickers as columns and dates as index
    pivoted_dataframe = dataframe.pivot(index='Date', columns='Ticker')
    
    # Calculate the percentage change between close and open prices for each sector
    returns_dataframe = ((pivoted_dataframe['Close'] - pivoted_dataframe['Open']) / pivoted_dataframe['Open']) * 100
    
    # Reshape the dataframe to have a 'sector' column
    daily_returns_dataframe = returns_dataframe.reset_index().melt(id_vars=['Date'], var_name='ticker', value_name='daily_return')
    
    return daily_returns_dataframe

@st.cache_data
def calculate_intraday_returns(dataframe):
    """
    Calculate returns using the opening price of the first observation in the day
    and the most recent close price for each security.

    Parameters:
        dataframe (pd.DataFrame): The input dataframe with columns 'Ticker', 'Timestamp', 'Open', 'Close'.

    Returns:
        pd.DataFrame: A dataframe containing 'Ticker', 'Close', 'Open', and 'Return'.
    """
    # Convert timestamp column to datetime
    dataframe['DateTime'] = pd.to_datetime(dataframe['DateTime'])

    # Filter data to get the opening price of the first observation in the day for each security
    first_open_data = dataframe.groupby(['Ticker', dataframe['DateTime'].dt.date])['Open'].first().reset_index()

    # Get the most recent close price for each security
    most_recent_data = dataframe.groupby('Ticker').apply(lambda group: group.nlargest(1, 'DateTime'))
    most_recent_data = most_recent_data.reset_index(drop=True)

    # Merge the opening price data with the most recent close price data
    merged_data = pd.merge(most_recent_data[['Ticker', 'Close']], first_open_data, 
                           on='Ticker', suffixes=('_close', '_open'))

    # Calculate returns
    merged_data['return'] = (merged_data['Close'] - merged_data['Open']) / merged_data['Open']

    return merged_data

@st.cache_data
def get_stock_info(stockDF):
    """
    Gets relevant fundamental data from each security from the dataframe stockDF

    Parameters:
        dataframe (pd.DataFrame): The input dataframe with columns 'Ticker'

    Returns:
        pd.DataFrame: A dataframe containing 'Ticker', 'market cap'
    """
    modifiedstockDF = stockDF.copy()
    modifiedstockDF["market cap"] = np.nan
    for index, row in modifiedstockDF.iterrows():
        ticker = row['Ticker']
        # retrieve the ticker info from yfinance api
        try:
            info = yf.Ticker(ticker).info
            if "marketCap" in info:
                modifiedstockDF.at[index, 'market cap'] = round((info['marketCap'] / 1000000000), 2)
        except:
            continue
    return modifiedstockDF

#RSI14
@st.cache_data
def RSI(df, n=14):
    close = df['Close']
    delta = close.diff()
    delta = delta[1:]
    pricesUp = delta.copy()
    pricesDown = delta.copy()
    pricesUp[pricesUp < 0] = 0
    pricesDown[pricesDown > 0] = 0
    rollUp = pricesUp.rolling(n).mean()
    rollDown = pricesDown.abs().rolling(n).mean()
    rs = rollUp / rollDown
    rsi = 100.0 - (100.0 / (1.0 + rs))
    return rsi