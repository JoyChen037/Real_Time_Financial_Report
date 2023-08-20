import sys
sys.path.append("C:/Users/joych/OneDrive/Desktop/trading/")
import streamlit as st
import pandas as pd
import plotly.graph_objs as go
from plotly.subplots import make_subplots
import plotly.express as px
import time
import numpy as np
import data
from util import *
import yfinance as yf
from operator import itemgetter

# Page configuration
st.set_page_config(
    page_title="Security",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state='collapsed'
)

# Styling for sidebar and container
st.markdown("""
<style>
    .block-container {
        padding-top: 1rem;
        padding-bottom: 0rem;
        padding-left: 3rem;
        padding-right: 3rem;
    }
    [data-testid="stMetricValue"] {
        font-size: 16px;
    }
    div[data-testid="stVerticalBlock"] > [style*="flex-direction: column;"] > [data-testid="stVerticalBlock"] {
        border: 1px solid #9DB2BF;
        padding: 10px 10px 20px 10px;
        border-radius: 3px;
        color: #E4E3E3;
        overflow-wrap: break-word;
    }
</style>
""", unsafe_allow_html=True)

# Company security selector
sp500_comp_df = data.read_from_s3(data.s3_client, 'airflow-stock-tickers-on-us-exchanges', 
                                  'ticker', 'parquet')
company = st.sidebar.selectbox('Select Security',sp500_comp_df['company'])

# Data loading and preprocessing
stock_price_df = data.read_from_s3(data.s3_client, 'sp500stockprice', 'sp_hist_rice', 'parquet')
stock_intraday_price_df = data.read_from_s3(data.s3_client, 'realtime-stock-price', 'stockstradingprice.csv', 'csv')
ticker = sp500_comp_df.loc[sp500_comp_df['company'] == company, 'ticker'].item()
ticker_intraday_price = stock_intraday_price_df.loc[stock_intraday_price_df['Ticker'] == ticker]
ticker_daily_price = stock_price_df.loc[stock_price_df['Ticker'] == ticker]
sector_price_df = data.read_from_s3(data.s3_client, 'sectorprice', 'sector_ETF_price', 'parquet')
spy_price_df = sector_price_df[sector_price_df['Ticker'] == 'SPY']
comp_news_df = data.read_from_s3(data.s3_client, 'sp500-stock-news', 'sp500news', 'parquet')
comp_news_df = comp_news_df[comp_news_df['ticker'] == ticker]

# Page title and description
st.markdown(f"# Security Overview: {company}")
st.caption(
    """On this page, you will find a wealth of data, including real-time trading prices, 
    economic indicators, breaking news, and an array of other essential details pertaining 
    to the security you are interested in."""
)

# Get ticker info and extract fundamental data
info = yf.Ticker(ticker).info
database = itemgetter('open', 'regularMarketPreviousClose', 'dayLow', 'dayHigh', 'bid', 
                  'ask', 'volume', 'regularMarketVolume', 'beta', 'trailingPE', 'forwardPE', 
                  'marketCap','dividendRate', 'dividendYield')(info)

# Display metrics using columns
col1, col2, col3 = st.columns([0.5,0.5, 2])
col1.metric("Open", database[0])
col2.metric("Previous Close", database[1])
col1.metric("Day Range", f"{database[2]}-{database[3]}")
col2.metric("Bid", database[4])
col1.metric("Ask", database[5])
col2.metric("Volume", format(database[6] / 1000000, '.2f') + "M")
col1.metric("Average Volumn", format(database[7] / 1000000, '.2f') + "M")
col2.metric("Beta", format(database[8], '.3f'))
col1.metric("Trailing PE", format(database[9], '.2f'))
col2.metric("Forward PE", format(database[10], '.3f'))
col1.metric("Market Cap", format(database[11] / 1000000000, '.2f') + "B")
col2.metric("Dividend (yield)", f"{database[12]} (" + format(database[13] * 100, '.2f') + "%)")

# Display OHLC plot
with col3:
    fig = make_subplots(
    rows=3, cols=1,
    specs=[[{"rowspan": 2}],
           [None],
           [{}]],print_grid=True)
    #OHLC Plot
    fig.add_trace(go.Ohlc(x=ticker_intraday_price.DateTime, open=ticker_intraday_price.Open, 
                          high=ticker_intraday_price.High, low=ticker_intraday_price.Low, 
                          close=ticker_intraday_price.Close, name='Price'), row=1, col=1)
    #Volume Plot
    fig.add_trace(go.Scatter(x=ticker_intraday_price.DateTime,
                             y=ticker_intraday_price.Volume, name='Volume'),
                  row=3, col=1)

    fig.update(layout_xaxis_rangeslider_visible=False)
    fig.update_layout(hovermode="x unified",
                    margin= dict(l=0,r=10,b=10,t=10),
                    width=900,
                    height=400)
    st.plotly_chart(fig, use_container_width=True)

st.divider()

# Display technical indicators
st.subheader("Technical Indicators")
st.caption("""Use SMA, EMA, RSI and other technical indicators to 
           get a general sense of the price trend""")

# Calculate EMA (Exponential Moving Average) with different windows
ticker_daily_price['EMA_9'] = ticker_daily_price['Close'].ewm(9).mean().shift()
ticker_daily_price['EMA_22'] = ticker_daily_price['Close'].ewm(22).mean().shift()

# Calculate SMA (Simple Moving Average) with different windows
ticker_daily_price['SMA_5'] = ticker_daily_price['Close'].rolling(5).mean().shift()
ticker_daily_price['SMA_10'] = ticker_daily_price['Close'].rolling(10).mean().shift()
ticker_daily_price['SMA_15'] = ticker_daily_price['Close'].rolling(15).mean().shift()
ticker_daily_price['SMA_30'] = ticker_daily_price['Close'].rolling(30).mean().shift()

# Create subplots layout for displaying multiple charts
fig = make_subplots(
    rows=3, cols=1,
    specs=[[{"rowspan": 2}],
           [None],
           [{}]], print_grid=True)

# Add traces for EMA and SMA on the first subplot
fig.add_trace(go.Scatter(x=ticker_daily_price.Date, y=ticker_daily_price.EMA_9, name='EMA 9'), row=1, col=1)
fig.add_trace(go.Scatter(x=ticker_daily_price.Date, y=ticker_daily_price.EMA_22, name='EMA 22'), row=1, col=1)
fig.add_trace(go.Scatter(x=ticker_daily_price.Date, y=ticker_daily_price.SMA_5, name='SMA 5'), row=1, col=1)
fig.add_trace(go.Scatter(x=ticker_daily_price.Date, y=ticker_daily_price.SMA_10, name='SMA 10'), row=1, col=1)
fig.add_trace(go.Scatter(x=ticker_daily_price.Date, y=ticker_daily_price.SMA_15, name='SMA 15'), row=1, col=1)
fig.add_trace(go.Scatter(x=ticker_daily_price.Date, y=ticker_daily_price.SMA_30, name='SMA 30'), row=1, col=1)
fig.add_trace(go.Scatter(x=ticker_daily_price.Date, y=ticker_daily_price.Close, name='Close', opacity=0.3), row=1, col=1)

# Calculate RSI (Relative Strength Index) and fill NaN values with 0
ticker_daily_price['RSI'] = RSI(ticker_daily_price).fillna(0)

# Add RSI trace on the third subplot
fig.add_trace(go.Scatter(x=ticker_daily_price.Date, y=ticker_daily_price.RSI, name='RSI'), row=3, col=1)

# Update layout of the figure
fig.update_layout(hovermode="x unified",
                  margin=dict(l=0, r=10, b=10, t=10),
                  width=900,
                  height=400)

# Display the plotly chart
st.plotly_chart(fig, use_container_width=True)


st.divider()

# Display price performance comparison chart
st.subheader("Relative Strength Performance Comparison")
st.caption("""A quick and easy way to analyze the performance and characteristics of different 
           stocks or securities""")

# Select another S&P500 company
option = st.selectbox(
    "Choose another S&P500 company",
    sp500_comp_df['company'],
    label_visibility="visible"
)

# Merge data for the main company's daily prices and relative prices with SPY
ticker_daily_price = ticker_daily_price.merge(spy_price_df, left_on='Date', right_on='Date')
ticker_daily_price['Relative Price'] = ticker_daily_price['Close_x'] / ticker_daily_price['Close_y']

# Get information for the second selected company
second_ticker = sp500_comp_df.loc[sp500_comp_df['company'] == option, 'ticker'].item()
secondticker_daily_price = stock_price_df.loc[stock_price_df['Ticker'] == second_ticker]
secondticker_daily_price = secondticker_daily_price.merge(spy_price_df, 
                                                          left_on='Date', 
                                                          right_on='Date')
secondticker_daily_price['Relative Price'] = secondticker_daily_price['Close_x'] / secondticker_daily_price['Close_y']

# Create a figure for plotting
fig = go.Figure()

# Add traces for both companies' relative prices
fig.add_trace(
    go.Scatter(
        x=ticker_daily_price['Date'],
        y=ticker_daily_price['Relative Price'],
        mode='lines',
        line=dict(color='#9DB2BF', width=2.5),
        name=company,
    )
)
fig.add_trace(
    go.Scatter(
        x=secondticker_daily_price['Date'],
        y=secondticker_daily_price['Relative Price'],
        mode='lines',
        line=dict(color='#CBB279', width=2.5),
        name=option,
    )
)

# Update the layout of the figure
fig.update_layout(
    hovermode="x unified",
    margin=dict(l=0, r=10, b=10, t=10),
    width=900,
    height=400
)

# Update the x-axis with range selector buttons
fig.update_xaxes(
    rangeslider_visible=False,
    rangeselector=dict(
        buttons=list([
            dict(count=1, label="1M", step="month", stepmode="backward"),
            dict(count=2, label="2M", step="month", stepmode="backward"),
            dict(count=3, label="3M", step="month", stepmode="todate"),
            dict(step="all")
        ])
    )
)

# Update the y-axis with a specified range
fig.update_yaxes(
    tickprefix='$',
    range=[
        min(
            min(secondticker_daily_price['Relative Price']),
            min(ticker_daily_price['Relative Price'])
        ) - 0.05,
        max(
            max(secondticker_daily_price['Relative Price']),
            max(ticker_daily_price['Relative Price'])
        ) + 0.05
    ]
)

# Display the plotly chart
st.plotly_chart(fig, use_container_width=True)

st.divider()

# Display financial news
st.header("Financial News")
st.caption("Most-up-to-date financial news scraped from across global news platforms with summaries for those on the run.")

num = 0
for index, row in comp_news_df.iterrows():
    col1, col2, col3 = st.columns([0.2, 0.1, 10])
    # Display news number
    col1.subheader(f"{num + 1}")
    num += 1
    
    with col3:
        with st.container():
            # Display news title
            st.subheader(f"{row['title']}")
            
            col4, col5, col6 = st.columns([0.4, 0.1, 4])
            col4.markdown(f"{row['date']}")
            col5.markdown("|")      
            col6.markdown(row['link'])
            with st.expander('Summary'):
                st.markdown(row['text'])
        # Add a separator line
        st.write("#")