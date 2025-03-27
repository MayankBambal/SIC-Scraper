import random
import time
import pandas as pd
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from selenium import webdriver
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from selenium.webdriver.firefox.service import Service as FirefoxService
from bs4 import BeautifulSoup

def get_company_data(headers=None):
    """
    Fetches company ticker data from the SEC, converts it to a DataFrame,
    and formats the CIK with leading zeros.

    Args:
        headers (dict, optional): Optional headers for the HTTP request (e.g., User-Agent).

    Returns:
        pd.DataFrame: DataFrame containing company tickers with formatted CIKs.
    """
    url = "https://www.sec.gov/files/company_tickers.json"
    response = requests.get(url, headers=headers)
    response.raise_for_status()  # Raise error if request fails

    company_data = pd.DataFrame.from_dict(response.json(), orient='index')
    company_data['cik_str'] = company_data['cik_str'].astype(str).str.zfill(10)

    return company_data

def get_missing_sic_rows(all_ciks_file):
    """
    Returns a DataFrame containing rows from the input file
    where SIC_code is missing or zero.

    Args:
        all_ciks_file (str): Path to the CSV file containing cik_str, ticker, title, SIC_code.

    Returns:
        pd.DataFrame: Filtered DataFrame with missing or zero SIC_code.
    """
    df = pd.read_csv(all_ciks_file)

    # Ensure SIC_code exists and is numeric
    if 'SIC_code' not in df.columns:
        df['SIC_code'] = 0
    else:
        df['SIC_code'] = pd.to_numeric(df['SIC_code'], errors='coerce').fillna(0).astype(int)

    # Filter rows with SIC_code == 0
    df_missing = df[df['SIC_code'] == 0]

    df_missing.reset_index(drop=True, inplace=True)

    return df_missing