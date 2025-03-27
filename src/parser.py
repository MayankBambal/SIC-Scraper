import random
import time
import pandas as pd
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from selenium import webdriver
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from selenium.webdriver.firefox.service import Service as FirefoxService
from bs4 import BeautifulSoup

headers = {'User-Agent': "mbambal@purdue.edu"}

def get_edgar_data_selenium(cik):
    """
    Uses Selenium with Firefox to load the SEC EDGAR page for the given CIK.
    Returns the rendered HTML.
    """
    # Set up Firefox options for headless mode
    firefox_options = FirefoxOptions()
    firefox_options.add_argument("--headless")
    firefox_options.add_argument("--no-sandbox")
    firefox_options.add_argument("--disable-dev-shm-usage")
    # Set the binary location (adjust if necessary)
    firefox_options.binary_location = "/Applications/Firefox.app/Contents/MacOS/firefox"
    
    # Create a Service object pointing to geckodriver (adjust the path if needed)
    service = FirefoxService(executable_path="/opt/homebrew/bin/geckodriver")
    
    # Initialize the Firefox WebDriver with the service and options
    driver = webdriver.Firefox(service=service, options=firefox_options)

    # Construct the SEC EDGAR URL for the given CIK (using the classic endpoint)
    url = f"https://www.sec.gov/cgi-bin/browse-edgar?CIK={cik}&owner=exclude&action=getcompany"
    driver.get(url)
    
    # Allow time for the page to load completely
    time.sleep(3)
    
    # Get the rendered HTML
    html = driver.page_source
    driver.quit()
    return html

def get_sic_code_from_html(html):
    """
    Parses the provided HTML using BeautifulSoup and extracts the SIC code.
    It looks for a <span> element with id="SIC" and returns its text.
    """
    soup = BeautifulSoup(html, 'html.parser')
    sic_spans = soup.find_all("span", id="SIC")
    if sic_spans:
        return sic_spans[0].get_text(strip=True)
    return None

def process_company(cik):
    html_data = get_edgar_data_selenium(cik)
    sic_code = get_sic_code_from_html(html_data) if html_data else None
    time.sleep(random.uniform(1, 3))  # Avoid hitting SEC too fast
    return cik, sic_code

def process_single_batch(companyData, batch_num, batch_size=1000):
    start = batch_num * batch_size
    end = start + batch_size
    batch_df = companyData.iloc[start:end].copy()
    print(f"\nProcessing batch {batch_num} (index {start} to {end})...\n")

    results = []
    total = len(batch_df)
    correct = 0

    for i, row in batch_df.iterrows():
        cik = row['cik_str']
        cik, sic_code = process_company(cik)

        # If parsing failed, store 0
        if not sic_code:
            sic_code = 0
        else:
            correct += 1

        companyData.at[i, 'SIC_code'] = sic_code
        results.append((cik, sic_code))

        # Show progress
        percent_complete = ((i - start + 1) / total) * 100
        percent_correct = (correct / (i - start + 1)) * 100
        print(f"Progress: {percent_complete:.2f}% | Correct: {percent_correct:.2f}%", end="\r")

    # Save result to CSV
    save_path = rf"/Users/mayankbambal/Desktop/SIC Scraper/data/stagging\Data_with_SIC_codes{batch_num}.csv"
    companyData.to_csv(save_path, index=False)
    print(f"\n\nBatch {batch_num} completed. Saved to:\n{save_path}\n")
    return companyData