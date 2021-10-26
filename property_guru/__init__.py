# less verbose logs
import logging

logging.getLogger("scrapy").propagate = False
