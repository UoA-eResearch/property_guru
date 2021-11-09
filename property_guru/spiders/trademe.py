import scrapy
from scrapy.http import FormRequest
from scrapy.exceptions import CloseSpider
import json
import logging
from pprint import pprint
import pandas as pd
from tqdm import tqdm
from datetime import datetime

logger = logging.getLogger("trademe")

LISTING_TYPE = "CommercialSale" # One of CommercialSale, CommercialLease, Residential, Rental, NewHomes, Rural, Lifestyle, Retirement. Not case sensitive

class TrademeSpider(scrapy.Spider):
    name = "trademe"
    allowed_domains = ["trademe.co.nz"]
    base_url = f"https://api.trademe.co.nz/v1/search/property/{LISTING_TYPE}.json"

    def start_requests(self):
        for page in range(1, 100):
            yield FormRequest(
                self.base_url,
                formdata = {
                    "page": str(page),
                    "rows": "500",
                },
                method = "GET",
                headers = {
                    "x-trademe-uniqueclientid": "f7e6fb2a-5629-aaee-34e2-9e17d7b2cfaa",
                    "Referer": "https://www.trademe.co.nz/",
                }
            )

    def parse_unix_timestamp(self, s):
        s = "".join(c for c in s if c.isnumeric())
        return datetime.fromtimestamp(int(s) / 1000)

    def parse(self, response):
        r = json.loads(response.text)
        logger.debug(f"Page {r['Page']}. PageSize: {r['PageSize']}. TotalCount: {r['TotalCount']}")
        if r["PageSize"] == 0:
            raise CloseSpider("finished")
        for listing in r["List"]:
            for k in ["StartDate", "EndDate", "AsAt", "NoteDate"]:
                listing[k] = self.parse_unix_timestamp(listing[k])
            yield listing