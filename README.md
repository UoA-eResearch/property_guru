# property_guru

Python scraper using scrapy to scrape listings from https://www.property-guru.co.nz/gurux/ or TradeMe's API (https://api.trademe.co.nz)

## Installation

`pip install -r requirements.txt`

## Running

### PropertyGuru
`scrapy crawl propertyguru -O PropertyGuru_Auckland.csv`

### Trademe
`scrapy crawl trademe -O TradeMe_CommercialSale.csv`

### Clearing the cache

`rm -rf .scrapy/httpcache`