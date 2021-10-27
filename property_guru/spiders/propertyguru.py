import scrapy
from scrapy.http import FormRequest
import json
import logging
from pprint import pprint
import pandas as pd
from tqdm import tqdm

logger = logging.getLogger("propertyguru")

with open("secrets.json") as f:
    secrets = json.load(f)

class PropertyguruSpider(scrapy.Spider):
    name = "propertyguru"
    allowed_domains = ["property-guru.co.nz"]
    base_url = "https://www.property-guru.co.nz/gurux/"
    render_url = base_url + "render.php"
    count = 0
    all_listing_ids = []

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
        for offset in tqdm(range(30020, -1, -20)):
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
                    "offset": str(offset),
                    "hid": "2",
                    "hash": "F7A5D3D1-3C65-22B7-3703-45BACD968874",
                    "fastSearch": "",
                },
                callback=self.handle_page,
            )

    def closed(self, reason):
        logger.debug(reason)
        with open("original_listing_ids", "w") as f:
            f.writelines(f"{line}\n" for line in self.all_listing_ids)

    def parse(self, response):
        pass

    def get_id(self, s):
        return "".join(c for c in s if c.isnumeric())

    def handle_page(self, response):
        logger.debug(f"Page: {response.css('.pager>span::text').get()}")
        links = response.css("div.listing a::attr(href)").getall()
        listing_ids = [self.get_id(l) for l in links]
        logger.debug(f"{len(listing_ids)} listing IDs found on this page: {listing_ids}")
        if len(set(listing_ids)) != len(listing_ids):
            logger.warning(f"{listing_ids} contains duplicates")
        for listing_id in listing_ids:
            if listing_id in self.all_listing_ids:
                logger.warning(f"{listing_id} is a duplicate")
                continue # scrapy would ignore a duplicate FormRequest anyway
            yield FormRequest(
                self.render_url,
                formdata={
                    "action": "MWUpdateQuery",
                    "mwDisplayOutput": "MWSummary",
                    "id": listing_id,
                    "viewCallback": "updateQueryMWsummary();",
                    "hid": "13",
                    "hash": "F0E7AAEA-35C6-06CC-BBEF-39985CA98C53",
                    "fastSearch": "",
                },
                callback=self.handle_listing,
                cb_kwargs={"listing_id": listing_id}
            )
        self.all_listing_ids.extend(listing_ids)

    def handle_listing(self, response, listing_id):
        try:
            history = pd.read_html(response.text, match="Date")[0].to_csv(index=False)
        except ValueError:
            history = None
        result = {
            "Listing_title": response.css("#property-teaser::text").get().strip(),
            "Listed_date": response.css("#property-listed-date::text").getall()[-1].split()[0],
            "Status": response.css("div.listing-name-status::text").get().strip(),
            "Address": response.css("#property-street-address>a::text").get(),
            "Ad_description": "\n".join(response.css("#property-description::text").getall()).strip(),
            "Price_method": response.css("#property-details-right > table > tbody > tr:nth-child(1) > td > strong::text").get(),
            "Floor_area": response.css("#property-details-right > table > tbody > tr:nth-child(2) > td::text").get(),
            "Listing_no": response.css("#property-details-right > table > tbody > tr:nth-child(3) > td::text").get(),
            "Val_ref": response.css("#property-details-right > table > tbody > tr:nth-child(4) > td > a:nth-child(1)::text").get(),
            "Agent_name": response.css("#property-agent-details h4::text").get(),
            "Agency_name": response.css("#property-agent-details span::text").get(),
            "Listing_history": history,
            "Listing_id": listing_id
        }
        #pprint(result)
        yield result