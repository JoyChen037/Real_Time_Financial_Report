#!/bin/bash
cd
cd OneDrive/Desktop/trading/pipeline
python -c 'from financial_data_fetcher import get_sp500_comp_list; get_sp500_comp_list()'