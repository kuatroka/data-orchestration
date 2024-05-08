

import os
import io
import itertools
import re
import random
import datetime
import time
from dotenv import load_dotenv
load_dotenv()
import yfinance as yf
import duckdb
import polars as pl
import pandas as pd
from pathlib import Path 
from ratelimit import limits
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
# from requests.packages.urllib3.util.retry import Retry

MD_02_MEDIAN_SEC_PRICE_FILE = Path(os.getenv('MD_02_MEDIAN_SEC_PRICE_FILE'))
MD_02_CUSIP_FTD_FILE = Path(os.getenv('MD_02_CUSIP_FTD_FILE'))
MD_02_YAHOO_PRICES = Path(os.getenv('MD_02_YAHOO_PRICES'))
## AV
MD_02_AV_PRICES = Path(os.getenv('MD_02_AV_PRICES'))
MD_02_AV_TICKER_NOT_AVAILABLE = Path(os.getenv('MD_02_AV_TICKER_NOT_AVAILABLE'))
## FMP
MD_02_FMP_PRICES = Path(os.getenv('MD_02_FMP_PRICES'))
MD_02_FMP_TICKER_NOT_AVAILABLE = Path(os.getenv('MD_02_FMP_TICKER_NOT_AVAILABLE'))

tickers = duckdb.sql(f"""
        SELECT cusip_ticker.ticker AS ticker
        FROM
        (SELECT DISTINCT cusip FROM read_parquet('{MD_02_MEDIAN_SEC_PRICE_FILE}'))  median_prices
        INNER JOIN
        (SELECT DISTINCT cusip, ticker FROM read_parquet('{MD_02_CUSIP_FTD_FILE}')) cusip_ticker
        ON median_prices.cusip = cusip_ticker.cusip
    """).pl().to_series().sort().to_list()

    # tickers = ['BRKB', 'TSLA']
    
malformed_substrings = ['/', '*']
ticker_doubles = {'BRKB':'BRK-B', 'BRKA':'BRK-A'}
tickers = [
    ticker_doubles.get(ticker, ticker)  # Use get() to handle potential absence in ticker_doubles
    for ticker in tickers
    if not any(char.isdigit() for char in ticker)
    and not any(substring in ticker for substring in malformed_substrings)
    # and not in not_in_av
    ]

# tickers = tickers[24298:]

# tickers = random.sample(tickers, 200)
# tickers = sorted(tickers) 
# print(tickers)


##################################################################################################
################################ YAHOO STOCK DATA DOWNLOAD #######################################
##################################################################################################


MINUTE = 60        
@limits(calls=20, period=MINUTE)
def fetch_yahoo_prices(tickers):
    print(f"\nYAHOO STOCK DATA Download started at: {time.ctime()}\n")
    start = time.perf_counter()
    
    
    print(f"Trying to download: {len(tickers)} tickers from YAHOO")

    groups = [tickers[i::7] for i in range(7)]
    # print(groups)

    # #  time period and interval
    # yf_period   = "25y"   # 1d,5d,1mo,3mo,6mo,1y,2y,5y,10y,ytd,max
    yf_interval = "3mo"    # 1m,2m,5m,15m,30m,60m,90m,1h,1d,5d,1wk,1mo,3mo   

    # Download the data for each group
    for group in groups:
        if not group:
            continue

        if len(group) ==  1:
            group.append('AAPL')
        
        df = (yf.download(tickers=group, interval=yf_interval, actions=True, threads=True, group_by='ticker')
        .reset_index().rename(columns={'Date': 'date'}).set_index('date'))
    
        data = df.T
        all_downloaded_tickers = list(set([idx[0] for idx in data.index]))
        # print(len(all_downloaded_tickers))
        
        for ticker in all_downloaded_tickers:
            df_out =  data.loc[ticker, :].T.sort_index().dropna().assign(ticker=ticker)
            df_out.columns = [column.lower().replace(' ', '_') for column in df_out.columns]
            if df_out.empty:
                continue
        
            # Check if 'stock_splits' and 'dividends' columns are present, if not add them
            if 'stock_splits' not in df_out.columns:
                df_out['stock_splits'] = 0.0
            
            if 'dividends' not in df_out.columns:
                df_out['dividends'] = 0.0
        
            if 'capital_gains' in df_out.columns:
                df_out = df_out.drop(columns=['capital_gains'])

            if (df_out['adj_close'] <= 0).any():
                print(f'Ticker {df_out["ticker"].iloc[0]} has negative or zero values in "adj_close". Skipping...')
            else:
                df_out = (df_out.assign(quarter=df_out.index.to_period(freq="Q").astype('string'),
                data_load=datetime.date.today()
                ))[['ticker', 'quarter', 'open', 'high', 
                            'low', "close", 'adj_close', 'volume',
                            'stock_splits', 'dividends', 'data_load']].sort_values(['quarter'], ascending=[False])
                df_out['volume'] = df_out['volume'].astype(int)
                df_out = df_out.rename(columns={'stock_splits': 'split_coefficient', 'dividends': 'dividend_amount', 'adj_close': 'adjusted_close'})
                df_out.index = (df_out.index  + pd.offsets.QuarterEnd())
                df_out.index = pd.to_datetime(df_out.index)
                
                df_out = df_out.assign(prc_change=(df_out['adjusted_close'].pct_change(-1)*100).round(2))
                first_adj_close = df_out['adjusted_close'].iloc[-1]
                df_out = df_out.assign(compound_return=(((df_out['adjusted_close'] - first_adj_close)/ first_adj_close)*100).round(2))
                
                last_trading_quarter = df_out.quarter.iloc[0]
                first_trading_quarter = df_out.quarter.iloc[-1]
                # print(df_out.tail())

                file_name_base = f'{MD_02_YAHOO_PRICES}/{ticker}_YAHOO_'
        
                # Check if any file starting with 'YAHOO_{ticker}' exists
                existing_files = list(Path(MD_02_YAHOO_PRICES).glob(f'{ticker}_YAHOO_*.parquet'))
                if len(existing_files) > 0:
                    [file.unlink() for file in existing_files]                
                
                df_out.to_parquet(f'{MD_02_YAHOO_PRICES}/{ticker}_YAHOO_{first_trading_quarter}_{last_trading_quarter}.parquet')

    print(f"\nYAHOO DATA Download finished at: {time.ctime()}\n")
    end = time.perf_counter()
    print(f"Download YAHOO DATA took: {(end-start)/60 :0.2f} min\n")


##################################################################################################
################################ AV STOCK DATA DOWNLOAD #######################################
##################################################################################################

def fetch_data_av(tickers):
    print(f"\nAV STOCK DATA Download started at: {time.ctime()}\n")
    start = time.perf_counter()

    api_keys_av = [value for key, value in os.environ.items() if re.match(r'API_KEY_AV_\d+', key)]
    # print(f'{len(api_keys_av)} AV keys')
    
    random.shuffle(api_keys_av)
    
    error_messages_rate = ['exceeded the DAILY quota', 'exceeded the rate limit per minute', 'not subscribed to this API']
    
    
    def is_internet_connected():
        try:
            requests.head("http://www.google.com", timeout=5)
            return True
        except requests.ConnectionError:
            return False
    

    
    ticker_idx_from = 0
    while ticker_idx_from < len(tickers):
        for key in itertools.cycle(api_keys_av):

            error_found = False
            while True:
                if is_internet_connected():
                    try:          
                        for ticker in tickers[ticker_idx_from:]:   
                            # print('##### AV #####')
                            # print(key)
                            # print(ticker)
                            url = "https://alpha-vantage.p.rapidapi.com/query"
                            querystring = {"symbol": f"{ticker}", "function": "TIME_SERIES_DAILY_ADJUSTED", "datatype": "csv", 'outputsize': 'full'}
                            headers = { "X-RapidAPI-Key": key,  "X-RapidAPI-Host": "alpha-vantage.p.rapidapi.com"}

                            retry_strategy = Retry(
                                total=30,
                                status_forcelist=[500, 502, 503, 504],
                                backoff_factor=10
                                # method_whitelist=["HEAD", "GET", "OPTIONS"]
                            )
                            adapter = HTTPAdapter(max_retries=retry_strategy)
                            http = requests.Session()
                            http.mount("https://", adapter)
                            http.mount("http://", adapter)
                            
                            start_time = time.time()
                            response = http.get(url, params=querystring, headers=headers, stream=True)
                            # time.sleep(max(0, 13 - (time.time() - start_time)))  # wait for 5 seconds to pass
                            time.sleep(max(0, 1 - (time.time() - start_time)))  # wait for 5 seconds to pass - let's try this one 
                            response_content = response.content
                            start_text = response_content[:100].decode('utf-8')

                            rate_limit_remaining_requests = response.headers.get('X-RateLimit-Requests-Remaining')
                            print(f'AV: ticker: {ticker} - key: {key} - Requests left: {rate_limit_remaining_requests}')
                            # print(start_text)
                
                            if any(error_message in start_text for error_message in error_messages_rate):
                                # do something if any of the error messages are found
                                print("Error message found! - Changing API key")
                                error_found = True

                                break
                            else:
                                # do something if none of the error messages are found
                                print("No error message found.")
                                polars_df = pl.read_csv(response_content)
                                polars_df = polars_df.with_columns(ticker=pl.lit(ticker).alias('ticker'))
                
                                if not polars_df.is_empty() and len(polars_df.columns) >= 9:                 
                
                                    quarterly_df = (
                                    polars_df.rename({'timestamp': 'date'}).with_columns(pl.col('date').str.strptime(pl.Datetime, "%Y-%m-%d")).sort(by='date')
                                    .group_by_dynamic(
                                        index_column="date",
                                        every="3mo",
                                        # offset="1m",
                                        # closed="right"
                                    )
                                    .agg(
                                        pl.col("ticker").first().alias("ticker"),
                                        pl.col("open").first().alias("open"),
                                        pl.col("high").max().alias("high"),
                                        pl.col("low").min().alias("low"),
                                        pl.col("close").last().alias("close"),
                                        pl.col("adjusted_close").last().alias("adjusted_close"),
                                        pl.col("volume").sum().alias("volume"),
                                        pl.col("dividend_amount").sum().alias("dividend_amount"),
                                        pl.col('split_coefficient').map_elements(lambda x: 1 if (x == 1).all() else min(v for v in x.unique() if v != 1), return_dtype=pl.Float64).alias('split_coefficient')
                                        ))
                            
                                    quarterly_df = (quarterly_df.with_columns([
                                            pl.when(pl.col("date").dt.month().is_in([1, 2, 3]))
                                            .then(pl.datetime(pl.col("date").dt.year(), 3, 31))
                                            .when(pl.col("date").dt.month().is_in([4, 5, 6]))
                                            .then(pl.datetime(pl.col("date").dt.year(), 6, 30))
                                            .when(pl.col("date").dt.month().is_in([7, 8, 9]))
                                            .then(pl.datetime(pl.col("date").dt.year(), 9, 30))
                                            .otherwise(pl.datetime(pl.col("date").dt.year(), 12, 31))
                                            .alias("last_day_of_quarter"),
                                            (pl.col('adjusted_close').pct_change()*100).round(2).alias('pct_change'),
                                            (((pl.col('adjusted_close') -  pl.col('adjusted_close').first().over('ticker'))/pl.col('adjusted_close').first().over('ticker'))*100).round(2).alias('compound_return'),
                                        ]).drop('date')
                                                )
                                
                                    quarterly_df = (quarterly_df.with_columns([(pl.col("last_day_of_quarter").dt.year().cast(pl.Utf8)+"Q"+pl.col("last_day_of_quarter").dt.quarter().cast(pl.Utf8)).alias("quarter"),
                                                                            pl.lit(datetime.datetime.now()).alias('data_load')
                                                        ]).rename({'last_day_of_quarter': 'date'})
                                                    [['ticker','quarter', 'open', 'high', 'low', 'close', 'adjusted_close', 'volume', "dividend_amount", "split_coefficient",'date', 'data_load', 'pct_change', 'compound_return']]
                                                    .sort('quarter', descending=True))
                
                                    last_trading_quarter = quarterly_df.filter(pl.col('adjusted_close').is_not_null()).get_column("quarter").head(1)[0]
                                    first_trading_quarter = quarterly_df.filter(pl.col('adjusted_close').is_not_null()).get_column("quarter").tail(1)[0] 
                
                                    file_path = os.path.join(MD_02_AV_PRICES, f'{ticker}_AV_{first_trading_quarter}_{last_trading_quarter}.parquet') 

                                    file_name_base = f'{MD_02_AV_PRICES}/{ticker}_AV_'
                    
                                    # Check if any file starting with '{ticker}_AV' exists
                                    existing_files = list(Path(MD_02_AV_PRICES).glob(f'{ticker}_AV_*.parquet'))
                                    if len(existing_files) > 0:
                                        [file.unlink() for file in existing_files]
                                        
                                    quarterly_df.write_parquet(file_path) 
                                    polars_df = None
                                    quarterly_df = None
                                    # print(polars_df.head(2))
                                    ticker_idx_from += 1
                
                
                                elif not polars_df.is_empty() and '{' in polars_df.columns:
                            
                                    if polars_df.get_column('{').str.contains('Invalid API call').any():
                                        polars_df = polars_df.with_columns(pl.lit(datetime.datetime.now()).alias('data_load'))
                                        # print(polars_df.head(1))
                                        file_path = os.path.join(MD_02_AV_TICKER_NOT_AVAILABLE, f'{ticker}_NOT_IN_AV.parquet')

                                        
                                        polars_df.write_parquet(file_path)
                                    
                                        # print(polars_df.head(2))
                                        polars_df = None
                                        ticker_idx_from += 1
                
                        
                            # ticker_idx_from += 1

                    except requests.RequestException as e:
                        print(f"Error: {e}")
                        time.sleep(10)  # wait for 10 seconds before retrying

                    
                if error_found:
                    break

                else:
                    print("Internet connection is down. Waiting for 30 seconds...")
                    time.sleep(30)  # wait for 30 seconds before retrying


                if ticker_idx_from > len(tickers) - 1:
                    break


            if ticker_idx_from > len(tickers) - 1:
                break

            # if error_found:
            #     break
        
    
        ticker_idx_from += 1

    print(f"\nAV STOCK DATA Download finished at: {time.ctime()}\n")
    end = time.perf_counter()
    print(f"\nAV Download took: {(end-start)/60 :0.2f} min\n")

##################################################################################################
################################ FMP STOCK DATA DOWNLOAD #######################################
##################################################################################################
def fetch_data_fmp(tickers):
    print(f"\nFMP STOCK DATA Download started at: {time.ctime()}\n")
    start = time.perf_counter()

    def is_internet_connected():
        try:
            requests.head("http://www.google.com", timeout=5)
            return True
        except requests.ConnectionError:
            return False    
    
    
    api_keys_fmp = [value for key, value in os.environ.items() if re.match(r'API_KEY_FMP_\d+', key)]
    
    random.shuffle(api_keys_fmp)
    
    error_messages_rate = ['exceeded the DAILY quota', 'exceeded the rate limit per minute', 'not subscribed to this API', 'Limit Reach']

    ticker_idx_from = 0
    while ticker_idx_from < len(tickers):
        for key in itertools.cycle(api_keys_fmp):
            error_found = False
            while True:
                if is_internet_connected():
                    try:
                        for ticker in tickers[ticker_idx_from:]:
                            # print('##### FMP #####')
                            # print(key)
                            # print(ticker)
                            url = f"https://financialmodelingprep.com/api/v3/historical-price-full/{ticker}?from=1900-04-27&apikey={key}"

                            retry_strategy = Retry(
                                total=30,
                                status_forcelist=[500, 502, 503, 504],
                                backoff_factor=10
                            )
                            adapter = HTTPAdapter(max_retries=retry_strategy)
                            http = requests.Session()
                            http.mount("https://", adapter)
                            http.mount("http://", adapter)

                            start_time = time.time()
                            response = http.get(url, stream=True)
                            # time.sleep(max(0, 2 - (time.time() - start_time)))  # wait for 2 seconds to pass - let's try this one
                            response_content = response.json()
                            start_text = response.text[:100]
                            print_text = response.text[:50]

                            print(f'FMP: ticker: {ticker} - key: {key} - print_text: {print_text}')

                            if any(error_message in start_text for error_message in error_messages_rate):
                                # do something if any of the error messages are found
                                print("Error message found!")
                                print(f'FMP:Error message found! - ticker: {ticker} - key: {key} - start_text: {start_text}')
                                print(f'response.headers: {response.headers}')
                                
                                error_found = True
                                break
                            else:
                                if len(response_content) > 0:   

                                    try:            
                                        polars_df = pl.from_dicts(response_content.get('historical'))
                                    except OverflowError as e:
                                        # Handle the overflow error
                                        print(f"Error: {e}. Skipping item {ticker}")
                                        continue
                                    
                                    polars_df = polars_df.with_columns(ticker=pl.lit(ticker).alias('ticker'))
                
                                    quarterly_df = (
                                    polars_df.rename({'adjClose': 'adjusted_close'}).with_columns(pl.col('date').str.strptime(pl.Datetime, "%Y-%m-%d")).sort(by='date')
                                    .group_by_dynamic(
                                        index_column="date",
                                        every="3mo",
                                        # offset="1m",
                                        # closed="right"
                                    )
                                    .agg(
                                        pl.col("ticker").first().alias("ticker"),
                                        pl.col("open").first().alias("open"),
                                        pl.col("high").max().alias("high"),
                                        pl.col("low").min().alias("low"),
                                        pl.col("close").last().alias("close"),
                                        pl.col("adjusted_close").last().alias("adjusted_close"),
                                        pl.col("volume").sum().alias("volume"),
                                        ))
                            
                                    quarterly_df = (quarterly_df.with_columns([
                                            pl.when(pl.col("date").dt.month().is_in([1, 2, 3]))
                                            .then(pl.datetime(pl.col("date").dt.year(), 3, 31))
                                            .when(pl.col("date").dt.month().is_in([4, 5, 6]))
                                            .then(pl.datetime(pl.col("date").dt.year(), 6, 30))
                                            .when(pl.col("date").dt.month().is_in([7, 8, 9]))
                                            .then(pl.datetime(pl.col("date").dt.year(), 9, 30))
                                            .otherwise(pl.datetime(pl.col("date").dt.year(), 12, 31))
                                            .alias("last_day_of_quarter"),
                                            (pl.col('adjusted_close').pct_change()*100).round(2).alias('pct_change'),
                                            (((pl.col('adjusted_close') -  pl.col('adjusted_close').first().over('ticker'))/pl.col('adjusted_close').first().over('ticker'))*100).round(2).alias('compound_return'),
                                        ]).drop('date')
                                                )
                                
                                    quarterly_df = (quarterly_df.with_columns([(pl.col("last_day_of_quarter").dt.year().cast(pl.Utf8)+"Q"+pl.col("last_day_of_quarter").dt.quarter().cast(pl.Utf8)).alias("quarter"),
                                                                            pl.lit(datetime.date.today()).alias('data_load')
                                                        ]).rename({'last_day_of_quarter': 'date'})
                                                    [['ticker','quarter', 'open', 'high', 'low', 'close', 'adjusted_close', 'volume', 'date', 'data_load', 'pct_change', 'compound_return']]
                                                    .sort('quarter', descending=True))
                
                                    last_trading_quarter = quarterly_df.filter(pl.col('adjusted_close').is_not_null()).get_column("quarter").head(1)[0]
                                    first_trading_quarter = quarterly_df.filter(pl.col('adjusted_close').is_not_null()).get_column("quarter").tail(1)[0] 
                
                                    file_path = os.path.join(MD_02_FMP_PRICES, f'{ticker}_FMP_{first_trading_quarter}_{last_trading_quarter}.parquet')  
            
            
                                    file_name_base = f'{MD_02_FMP_PRICES}/{ticker}_FMP'
                    
                                    # Check if any file starting with 'YAHOO_{ticker}' exists
                                    existing_files = list(Path(MD_02_FMP_PRICES).glob(f'{ticker}_FMP_*.parquet'))
                                    if len(existing_files) > 0:
                                        [file.unlink() for file in existing_files]                
                                    
                                    quarterly_df.write_parquet(file_path) 
                                    polars_df = None
                                    quarterly_df = None
                                    response_content = None
                                    # print(polars_df.head(2))
                                    ticker_idx_from += 1
                            
                            
                                elif len(response_content) == 0:
                                    # print('inside {}')
                                    polars_df = pl.from_dict({'ticker': ticker, 'data_load': datetime.date.today()})
                            
            
                                    # print(polars_df.head(1))
                                    file_path = os.path.join(MD_02_FMP_TICKER_NOT_AVAILABLE, f'{ticker}_NOT_IN_FMP.parquet')
                                    polars_df.write_parquet(file_path)
                                
                                    # print(polars_df.head(2))
                                    polars_df = None
                                    response_content = None
                                    ticker_idx_from += 1                            

                    
                    except requests.RequestException as e:
                        print(f"Error: {e}")
                        time.sleep(10)  # wait for 10 seconds before retrying
                else:
                    print("Internet connection is down. Waiting for 30 seconds...")
                    time.sleep(30)  # wait for 30 seconds before retrying

                if error_found:
                    break

                if ticker_idx_from > len(tickers) - 1:
                    break

            if ticker_idx_from > len(tickers) - 1:
                break

        ticker_idx_from += 1

    print(f"\nFMP STOCK DATA Download finished at: {time.ctime()}\n")
    end = time.perf_counter()
    print(f"Download FMP STOCK DATA took: {(end-start)/60 :0.2f} min\n")

if __name__ == "__main__":

    pass