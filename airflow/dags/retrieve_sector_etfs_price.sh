#!/bin/bash
cd
cd OneDrive/Desktop/trading/pipeline
python -c 'from financial_data_fetcher import get_sector_etf_historical_price; get_sector_etf_historical_price()'