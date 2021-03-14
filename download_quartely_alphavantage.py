#!/usr/bin/env python3
#
# Created 2021/03 by mdufkova
# Description: Script for downloading quarterly data from https://www.alphavantage.co/ API.
# Usage: /home/data/scripts/api/bin/python /home/data/scripts/download_quartely_alphavantage.py
# Server: ib@46.101.242.92:/home/data/scripts
# Cron: 0 10 15 1,4,7,10 * /home/data/scripts/api/bin/python /home/data/scripts/download_daily_data_api_alphavantage.py >>/home/data/scripts/download_daily_data_api_alphavantage.log 2>&1

import requests
import pandas as pd
import os.path
import logging
import time

path = os.path.dirname(os.path.abspath(__file__))  # path where the script is located
path_to_files = f"{path}/alphavantage/quarterly/INCOME_STATEMENT/"
path_to_tickers = f"{path}/tickers"
api_key = "SHFQHLWWKT91PO2K"
function = "INCOME_STATEMENT"  # to download daily data, others function to download quarterly data could be 'INCOME_STATEMENT', 'BALANCE_SHEET', 'CASH_FLOW' or 'EARNINGS'
list_repeated_items = []  # Tickers that failed.
logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)
headers = {
    'Content-Type': 'application/json'
}
logging.basicConfig(format='%(asctime)s %(message)s', level=logging.DEBUG)


def open_file():
    f = open(f"{path_to_tickers}/russel3000-incomest.txt", "r")
    return f


def close_file(f):
    f.close()


def api_request(ticker: str) -> dict:
    """Send api request and save body of response"""

    request_response = requests.get(
        f"https://www.alphavantage.co/query?function={function}&symbol={ticker}&apikey={api_key}", headers=headers)
    result_of_api_request = request_response.json()
    return result_of_api_request


def upload_parquet_file(ticker: str) -> pd.DataFrame:
    """ Loading data from parquet file as pandas data frame."""

    old_data = pd.read_parquet(f"{path_to_files}/{ticker}_quarterly.parquet")
    old_data = sort_data(old_data)
    return old_data


def save_file(data_to_add: pd.DataFrame, ticker: str):
    data_to_add = sort_data(data_to_add)
    data_to_add.to_parquet(f"{path_to_files}/{ticker}_quarterly.parquet", compression=None, engine='pyarrow',
                           use_deprecated_int96_timestamps=True)


def sort_data(data: pd.DataFrame) -> pd.DataFrame:
    data.sort_values("calendarDate", ascending=False, inplace=True)
    data.reset_index(drop=True, inplace=True)
    return data


def data_selection(ticker: str, result_of_api_request: dict) -> pd.DataFrame:
    """Choosing only the required daily data from result_of_api_request"""

    if not bool(result_of_api_request):
        logging.warning(f"!!! warning: No downloaded data for {ticker}!!!")
        if ticker not in list_repeated_items:
            list_repeated_items.append(ticker)
            logging.info(result_of_api_request)
    # add the current day
    else:
        new_data = pd.json_normalize(result_of_api_request, record_path=['quarterlyReports'])
        save_file(ticker, new_data)


def save_file(ticker: str, new_data: pd.DataFrame):
    new_data.to_parquet(f"{path_to_files}/{ticker}_quarterly.parquet")


def main():
    logging.info("script started")
    f = open_file()
    lines = f.readlines()
    count = 1
    for line in lines:
        if count <= 500:
            ticker = line.strip()
            ticker = ticker.replace(".", "-")
            logging.info(f"working on request: {count} and tickers: {ticker}")
            result_of_api_request = api_request(ticker)
            data_selection(ticker, result_of_api_request)
            count += 1
            time.sleep(14)
    close_file(f)
    logging.info(f"Tickers that failed {list_repeated_items}")
    logging.info("script ended")


if __name__ == "__main__":
    main()

# https://www.alphavantage.co/query?function=INCOME_STATEMENT&symbol=MSG&apikey=SHFQHLWWKT91PO2K