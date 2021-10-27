# property_guru

Python scraper using scrapy to scrape listings from https://www.property-guru.co.nz/gurux/

## Installation

`pip install -r requirements.txt`

## Running

`scrapy crawl propertyguru -O Auckland.csv`

### Clearing the cache

`rm -rf .scrapy/httpcache`