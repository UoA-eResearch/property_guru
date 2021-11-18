import scrapy
from scrapy.http import FormRequest
from scrapy.exceptions import CloseSpider
import json
import logging
from pprint import pprint
import pandas as pd
from tqdm import tqdm

logger = logging.getLogger("propertyguru")

SUBURBS = {
    # region_id, district_id, suburb_id
    "Auckland": ["2", "76", "2728"],
    "Wellington": ["9", "47", "2573"],
    "Christchurch": ["13", "60", "2081"],
    "Christchurch District All Suburbs": ["13", "60", ""]
}
LISTING_TYPES = {
    "Residential Sale": "10",
    "Residential Rental": "11",
    "Commercial Sale": "12",
    "Commercial Lease": "13",
    "Rural Sale": "14",
    "Sold": "90"
}
TARGET_SUBURB = SUBURBS["Christchurch District All Suburbs"]
LISTING_TYPE_ID = LISTING_TYPES["Residential Rental"]

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
        loginRequest = FormRequest(
            self.base_url,
            formdata={
                "login": "1",
                "Login": "Login",
                "user": secrets["user"],
                "password": secrets["password"],
                "rememberPassword": "on",
            },
            callback=self.handle_login
        )
        loginRequest.meta['dont_cache'] = True
        yield loginRequest

    def handle_login(self, response):
        assert "$user" in response.text
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
            for offset in range(0, 159151, 20):
                yield FormRequest(
                    self.render_url,
                    formdata={
                        "action": "MWUpdateQuery",
                        "mwDisplayOutput": "MWTable",
                        "region_id": TARGET_SUBURB[0],
                        "listed_in": "1960-01-01|2022-01-01",  # All time
                        "district_id": TARGET_SUBURB[1],
                        "listing_status": "all",
                        "suburb_id": TARGET_SUBURB[2],
                        "listing_type_id": LISTING_TYPE_ID,
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
        if len(listing_ids) == 0:
            raise CloseSpider('finished')
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