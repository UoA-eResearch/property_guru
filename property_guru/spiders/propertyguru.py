import scrapy
from scrapy.http import FormRequest
import json
import logging
from pprint import pprint
import pandas as pd
from tqdm import tqdm

logger = logging.getLogger("propertyguru")

try:
    with open("ids_to_refetch") as f:
        ids_to_refetch = [line.strip() for line in f]
        print(f"Re-fetching {ids_to_refetch}")
except:
    ids_to_refetch = None

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
        if ids_to_refetch:
            for listing_id in tqdm(ids_to_refetch):
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
        else:
            for offset in tqdm(range(33400, -1, -20)):
                yield FormRequest(
                    self.render_url,
                    formdata={
                        "action": "MWUpdateQuery",
                        "mwDisplayOutput": "MWTable",
                        "region_id": "2",  # Auckland Region
                        "listed_in": "1960-01-01|2021-10-30",  # All time
                        "district_id": "76",  # Auckland City
                        "listing_status": "all",  # options = all, 1 (Active), 2 (Withdrawn)
                        "suburb_id": "2728",  # Auckland Central
                        "listing_type_id": "13",  # Commercial Lease
                        "offset": str(offset),
                        "hid": "21",
                        "hash": "83751256-66D9-FE61-6C8C-E87EDEBFB5D9",
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
        assert len(response.text) > 112
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
            tables = pd.read_html(response.text)
            df = tables[0]
            df.columns = ["key", "value"]
            info = {r[1].key: r[1].value for r in df.iterrows()}
            try:
                history = tables[1].to_csv(index=False)
            except IndexError:
                history = None
        except ValueError as e:
            logger.error(e)
            logger.debug(response.text)
            raise
        result = {
            "Listing_title": response.css("#property-teaser::text").get().strip(),
            "Listed_date": response.css("#property-listed-date::text").getall()[-1].split()[0],
            "Status": response.css("div.listing-name-status::text").get().strip(),
            "Address": response.css("#property-street-address>a::text").get(),
            "Ad_description": "\n".join(response.css("#property-description::text").getall()).strip(),
            "Price_method": info.get("Price"),
            "Floor_area": info.get("Floor Area m2:"),
            "Listing_no": info.get("Listing No.:"),
            "Val_ref": info.get("Valuation ref.:"),
            "Agent_name": response.css("#property-agent-details h4::text").get(),
            "Agency_name": response.css("#property-agent-details span::text").get(),
            "Listing_history": history,
            "Listing_id": listing_id
        }
        #pprint(result)
        yield result