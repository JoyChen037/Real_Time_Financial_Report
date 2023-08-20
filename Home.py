import streamlit as st
import pandas as pd
import data
import s3fs
from s3fs import S3FileSystem
from util import *
import boto3
import collections
from decimal import Decimal
from pandas.tseries.offsets import BDay
import plotly.graph_objs as go
import plotly.express as px
from plotly.subplots import make_subplots
from st_aggrid import GridOptionsBuilder, AgGrid, GridUpdateMode, DataReturnMode, ColumnsAutoSizeMode

import json
# s3_clientobj = data.s3_client.get_object(Bucket='realtime-stock-price', Key='trade_price_1.json')
# s3_clientdata = s3_clientobj['Body'].read().decode('utf-8')
# print("printing s3_clientdata")
# st.table(s3_clientdata)

# Page configuration
st.set_page_config(
    page_title="Stock Market Report",
    page_icon=None,
    layout="wide",
    initial_sidebar_state='collapsed',
    menu_items=None
)

# Styling for sidebar and container
st.markdown("""
<style>
    div[data-testid="stSidebar"] {
        background-color: #1B1C25;
        color: #E4E3E3;
    }
    div[data-testid="metric-container"] {
        background-color: #1B1C25;
        border: 1px solid rgba(28, 131, 225, 0.1);
        padding: 5% 5% 5% 10%;
        border-radius: 5px;
        color: #E4E3E3;
        overflow-wrap: break-word;
    }
    .block-container {
        padding-top: 1rem;
        padding-bottom: 0rem;
        padding-left: 3rem;
        padding-right: 3rem;
    }
    .stSlider {
        margin-top: 0px;
    }
    [data-baseweb="select"] {
        margin-top: -25px;
    }
</style>
""", unsafe_allow_html=True)

# Data loading
sector_dict = {'sector': ['Materials', 'Communication Services', 'Energy', 'Financials',
                         'Industrials', 'Technology', 'Consumer Staples', 'Real Estate', 
                         'Utilities','Health Care', 'Consumer Discretionary'],
              'ticker': ['XLB', 'XLC', 'XLE', 'XLF', 'XLI', 'XLK', 
                         'XLP', 'XLRE', 'XLU', 'XLV', 'XLY']}
sectors_DF = pd.DataFrame.from_dict(sector_dict)
sp500_comp_DF = data.read_from_s3(data.s3_client,
                                  'airflow-stock-tickers-on-us-exchanges', 'ticker', 'parquet')
sectorPriceDF = data.read_from_s3(data.s3_client, 'sectorprice', 'sector_ETF_price', 'parquet')

# Real-time datasets
stock_price_df = data.read_from_s3(data.s3_client, 'realtime-stock-price', 'stockstradingprice.csv', 'csv')
sp500_df = data.read_from_s3(data.s3_client, 'sp500-real-time-price', 'sp500tradeprice.csv', 'csv')

# Calculate daily returns of each sector ETF
dailyETFReturn = calculate_daily_returns(sectorPriceDF)
sector_return = dailyETFReturn.merge(sectors_DF, left_on='ticker', right_on='ticker')

# Calculate introday return of each S&P500 securities
stocks_intra_return_df = calculate_intraday_returns(stock_price_df)

# Title and styling
st.title('Stock Market Summary')
st.caption("Intraday market price action, biggest movers, sectors performance, and more.")

# Market summary metrics
sp500_return = (sp500_df.Close.iloc[-1] - sp500_df.Open.iloc[0]) / sp500_df.Close.iloc[-1]
market_status = "Bullish" if sp500_return >= 0 else "Bearish"

# Top/worst stock
top_stock = stocks_intra_return_df['Ticker'].iloc[stocks_intra_return_df['return'].idxmax()]
top_return = max(stocks_intra_return_df['return'])
worst_stock = stocks_intra_return_df['Ticker'].iloc[stocks_intra_return_df['return'].idxmin()]
worst_return = min(stocks_intra_return_df['return'])
top_sector = sector_return['sector'].iloc[sector_return['daily_return'].idxmax()]
worst_sector = sector_return['sector'].iloc[sector_return['daily_return'].idxmin()]
top_sector_return = max(sector_return['daily_return'])
worst_sector_return = min(sector_return['daily_return'])

# Display metrics using columns
col1, col2, col3, col4, col5 = st.columns(5)
col1.metric("Market", market_status, format(sp500_return, ".3f") + "%")
col2.metric("Best Sector", top_sector, format(top_sector_return, ".3f") + "%")
col3.metric("Worst Sector", worst_sector, format(worst_sector_return, ".3f") + "%")
col4.metric("Top Stock", top_stock, format(top_return, ".3f") + "%")
col5.metric("Worst Stock", worst_stock, format(worst_return, ".3f") + "%")

st.divider()

# Display charts
col7, col, col8 = st.columns([1.5,0.1, 2])
with col7:
    # Area chart for SPY index
    st.subheader('SPY Index')
    area_chart = go.Figure(go.Scatter(x=sp500_df["Datetime"], y=sp500_df["Close"], mode='lines',
                                    fill='tozeroy', fillcolor='#413F42',line=dict(color="#EAEAEA")))
    area_chart.update_xaxes(title_text = 'Date')
    area_chart.update_yaxes(tickprefix = '$', range = [min(sp500_df['Open'])-25, max(sp500_df['Open']+25)])
    area_chart.update_layout(title=dict(text="Intraday price chart for S&P500", font=dict(size=15)),
                           hovermode="x unified", margin= dict(l=0,r=15,b=0,t=35),
                           showlegend = False, yaxis_title=None, height=380)
    st.plotly_chart(area_chart, use_container_width=True)

with col8:
    # Candlestick chart for selected stock
    col9, col10 = st.columns([2,1])
    company = col10.selectbox('stock', sp500_comp_DF['company'], label_visibility="hidden")
    col9.subheader(f'Quick Charts: {company}')
    ticker = sp500_comp_DF.loc[sp500_comp_DF['company'] == company, 'ticker'].item()
    ticker_price = stock_price_df.loc[stock_price_df['Ticker'] == ticker]
    fig = go.Figure()

    # candlestick
    fig.add_trace(
        go.Candlestick(
            x=ticker_price.DateTime,
            open=ticker_price.Open,
            high=ticker_price.High,
            low=ticker_price.Low,
            close=ticker_price.Close,
            showlegend=False)
    )

    fig.update_layout(title=dict(text="Daily price action for your stock of choice",
                                font=dict(size=15),
                                pad = dict(b = 10, l = 0, r = 0)),
                        yaxis_title="Price (USD)",
                        hovermode="x unified",
                        margin= dict(l=0,r=10,b=10,t=30),
                        width=900,
                        height=350)

    # hide the slide bar
    fig.update(layout_xaxis_rangeslider_visible=False)
    st.plotly_chart(fig, use_container_width=True)

st.divider()

# Display the market cap tree-map
st.subheader("MARKET OVERVIEW")
stocks_intra_return_df = stocks_intra_return_df.merge(sp500_comp_DF, left_on='Ticker', right_on='ticker')
sp500_comp_with_info_DF = get_stock_info(stocks_intra_return_df)
tree_map_df = sp500_comp_with_info_DF.sort_values('market cap', ascending = False).groupby('sector').head(7)
tree_map_df['return'] = tree_map_df['return'] * 100

fig = px.treemap(tree_map_df,
                 path=[px.Constant("Sectors"), 'sector', 'ticker'],
                 values='market cap',
                 color='return',
                 hover_data=['return', 'market cap'],
                 color_continuous_scale=['#771515', '#9A1A1A', '#BC2020', '#DA2A2A', "#E04C4C",
                                        '#000000', '#BCB88A', '#8A9A5B', '#355E3B', '#014421', '#013220'],
                 color_continuous_midpoint=0)
fig.data[0].texttemplate = "%{label}<br>%{customdata[0]:.2f}%"
fig.update_layout(title=dict(text="Overview of the biggest market cap stocks in each sector",
                             font=dict(size=15)),
                  hovermode = False,
                  paper_bgcolor='rgba(0,0,0,0)',
                  plot_bgcolor='rgba(0,0,0,0)', 
                  margin= dict(l=0,r=10,b=0,t=40),
                  width=900,
                  height=500)
st.plotly_chart(fig, use_container_width=True)

st.text("")
st.text("")
st.text("")
st.divider()

# Display sector charts
col11, col12, col13 = st.columns([1.5, 0.45, 4])
col11.subheader("SECTOR ROTATIONS")
# Sector selector
sectorOptions = col13.multiselect('Select Sectors', sector_dict['sector'],
                                  ['Technology', 'Health Care'], 
                                  label_visibility = "visible")
# Sector rotations historical line chart
sectorRangeDF = sector_return[(sector_return['sector'].isin(sectorOptions))]
fig = px.line(sectorRangeDF, 
              x='Date', 
              y='daily_return', 
              color='sector', 
              width=10, 
              line_shape='linear')
fig.update_xaxes(
    rangeslider_visible=False,
    rangeselector=dict(
        buttons=list([
            dict(count=1, label="1M", step="month", stepmode="backward"),
            dict(count=2, label="2M", step="month", stepmode="backward"),
            dict(count=3, label="3M", step="month", stepmode="todate"),
            dict(step="all")
        ]),
        x=0.35,  # Adjust the x position of the range selector buttons (0 is left, 1 is right)
        y=1.02
    ),
    tickformat='%Y-%m-%d'
)
# Add annotations for the start and end dates
end_date = sectorRangeDF['Date'].max()
annotations = []
y_shift = -15
x_shift = -10
spacing = 15

for ticker in sectorRangeDF['sector'].unique():
    sector_data = sectorRangeDF[sectorRangeDF['sector'] == ticker]
    end_return = sector_data[sector_data['Date'] == end_date]['daily_return'].values[0]
    annotations.append(
        dict(
            x=end_date,
            y=end_return,
            xref="x",
            yref="y",
            text=f"{ticker}: {end_return:.2f}%",
            showarrow=False,
            arrowhead=0,
            ax=0,
            ay=0,
            xshift=x_shift,  # Shift the annotation outside the plot
            yshift=y_shift,
            font=dict(color='white'),  # Set font color for text
            bgcolor='rgba(0, 0, 0, 0.5)',  # Transparent background color
            borderpad=4,  # Adjust padding around the text
            # bordercolor=ticker  # Use the same color as the legend
        )
    )
    y_shift += spacing
    x_shift += 15
   
fig.update_layout(
  title=dict(text = "How different sector ETFs have moved historically",
             font=dict(size=15),
             pad = dict(b = 0, l = 0, r = 0)),
  margin= dict(l=0,r=10,b=0,t=10),
  width=1100,
  height=500,
  legend=dict(font = dict(family = "Courier", size = 10, color = "white")),
  annotations=annotations
)

st.plotly_chart(fig)

# with col15:
#    # Sector performance bar chart
#   st.subheader("SECTOR PERFORMANCE")
#   sectorReturn['color'] = ['#B31312' if x < 0 else '#5D9C59' for x in sectorReturn['return']]
#   sectorReturn = sectorReturn.sort_values('return')
#   df = px.data.tips()
#   fig = go.Figure()
#   annotations = []
#   for xd, yd,color in zip(sectorReturn['return'], sectorReturn['sector'], sectorReturn['color']):
#     fig.add_trace(go.Bar(
#         x=[xd], y=[yd],
#         orientation='h',
#         marker=dict(
#             color=color,
#             # line=dict(color='rgb(248, 248, 249)', width=1)
#         )
#     ))
#   fig.update_layout(title=dict(text="Percentage change of each sector ETF today",
#                             font=dict(size=15)),
#                     margin= dict(l=0,r=10,b=10,t=40),
#                     showlegend=False,width=350,height=430)
#   # fig = px.bar(sectorReturn.sort_values('return'), x="return", y="sector", orientation='h')
#   st.plotly_chart(fig)

st.divider()

# Display top gainers and losers
col16, col17, col18 = st.columns([2, 0.1, 2])

# Format values and rename columns
sp500_comp_with_info_DF = sp500_comp_with_info_DF.drop(['ticker'], axis=1)
sp500_comp_with_info_DF['% Change'] = sp500_comp_with_info_DF['return'].apply(
  lambda num: "{:.3f}%".format(num))
sp500_comp_with_info_DF['market cap'] = sp500_comp_with_info_DF['market cap'].apply(
  lambda num: "{:.3f}M".format(num))
column_mapper = lambda col: col.capitalize() if col != "% Change" else col
sp500_comp_with_info_DF.rename(columns=column_mapper, inplace=True)

# Get the dataframes of gainers and losers
gainerDF = sp500_comp_with_info_DF[sp500_comp_with_info_DF['Return'] > 0] \
             .sort_values('Return', ascending=False)
loserDF = sp500_comp_with_info_DF[sp500_comp_with_info_DF['Return'] < 0] \
            .sort_values('Return', ascending=True)

# Leaders table
with col16: 
    st.subheader('LEADERS')
    st.caption("Strongest Performers Today")
    gb = GridOptionsBuilder.from_dataframe(gainerDF[['Ticker','Company',  "% Change", "Market cap"]])
    gb.configure_side_bar() #Add a sidebar
    gridOptions = gb.build()

    custom_css = {
        ".ag-row-hover": {"background-color": "#0E2954"},
        ".ag-header-cell-label": {"font-size": "13px", "color": "#B7B7B7"}
    }
  
    AgGrid(
        gainerDF[['Ticker','Company', "% Change", "Market cap"]],
        gridOptions=gridOptions,
        data_return_mode='AS_INPUT', 
        update_mode='MODEL_CHANGED', 
        fit_columns_on_grid_load=False,
        custom_css=custom_css,
        theme='streamlit', #Add theme color to the table
        columns_auto_size_mode=ColumnsAutoSizeMode.FIT_CONTENTS,
        enable_enterprise_modules=True,
        height=350, 
        width='100%',
        reload_data=True
    )

# Losers table
with col18:
    st.subheader('LAGGARDS')
    st.caption("Weakest Performers Today")
    gb = GridOptionsBuilder.from_dataframe(loserDF[['Ticker','Company',  "% Change", "Market cap"]])
    gb.configure_side_bar() #Add a sidebar
    gridOptions = gb.build()

    AgGrid(
        loserDF[['Ticker','Company', "% Change", "Market cap"]],
        gridOptions=gridOptions,
        data_return_mode='AS_INPUT', 
        update_mode='MODEL_CHANGED', 
        fit_columns_on_grid_load=False,
        columns_auto_size_mode=ColumnsAutoSizeMode.FIT_CONTENTS,
        custom_css=custom_css,
        theme='streamlit', #Add theme color to the table
        enable_enterprise_modules=True,
        height=350, 
        width='60%',
        reload_data=True
    )
