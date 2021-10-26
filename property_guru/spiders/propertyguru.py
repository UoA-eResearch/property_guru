import scrapy
from scrapy.http import FormRequest
import json
import logging

logger = logging.getLogger("propertyguru")

with open("secrets.json") as f:
    secrets = json.load(f)


class PropertyguruSpider(scrapy.Spider):
    name = "propertyguru"
    allowed_domains = ["property-guru.co.nz"]
    base_url = "https://www.property-guru.co.nz/gurux/"
    render_url = base_url + "render.php"

    def start_requests(self):
        yield FormRequest(
            self.base_url,
            formdata={
                "login": "1",
                "Login": "Login",
                "user": secrets["user"],
                "password": secrets["password"],
                "rememberPassword": "on",
            },
        )
        yield FormRequest(
            self.render_url,
            formdata={
                "action": "MWUpdateQuery",
                "mwDisplayOutput": "MWTable",
                "region_id": "2",  # Auckland Region
                "listed_in": "1960-01-01|2021-10-26",  # All time
                "district_id": "76",  # Auckland
                "listing_status": "2",  # options = all, 1 (Active), 2 (Withdrawn)
                "suburb_id": "2728",  # Auckland Central
                "listing_type_id": "13",  # Commercial Lease
                "offset": "30020",
                "hid": "2",
                "hash": "F7A5D3D1-3C65-22B7-3703-45BACD968874",
                "fastSearch": "",
            },
            callback=self.handle_page,
        )

    def parse(self, response):
        pass

    def handle_page(self, response):
        logger.debug(response.css(".pager>span::text").get())
