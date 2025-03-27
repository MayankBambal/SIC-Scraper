from getcikstoparse import get_missing_sic_rows
from parser import process_single_batch
from writetomain import update_sic_from_file
import pandas as pd

find_sic = get_missing_sic_rows('/Users/mayankbambal/Desktop/SIC Scraper/data/final/company_tickers.csv')
print(find_sic.shape)

batch_size = 1000
num_batches = find_sic.shape[0] // batch_size + 1
print(f"Processing {num_batches} batches of {batch_size} rows each...")
for batch_num in range(num_batches):
    process_single_batch(find_sic, batch_num, batch_size)
print("Processing complete.")

mainfile = pd.read_csv('/Users/mayankbambal/Desktop/SIC Scraper/data/final/company_tickers.csv')

# get all the files in stagging folder
scrapedfile = pd.read_csv('/Users/mayankbambal/Desktop/SIC Scraper/data/final/scraped_sic_codes.csv')
update_sic_from_file(mainfile, scrapedfile)






