"""
Wikipedia pageviews spark app configuration
Parse input env vars
"""
import os, sys, logging
from pyspark.sql.types import LongType, StringType, StructType
from datetime import datetime
from distutils.util import strtobool

APP_NAME = os.getenv("APP_NAME", "wiki_pageviews")
SOURCE_URL_PREFIX = os.getenv(
    "SOURCE_URL_PREFIX", "https://dumps.wikimedia.org/other/pageviews"
)
BLACKLIST_URL_IN = os.getenv(
    "BLACKLIST_URL_IN",
    "https://s3.amazonaws.com/dd-interview-data/data_engineer/wikipedia/blacklist_domains_and_pages",
)

PATH_OUT_PREFIX = os.getenv("PATH_OUT_PREFIX", "/mnt/out")
HOURLY = strtobool(os.getenv("HOURLY", "True"))
TOP_RANK = int(os.getenv("TOP_RANK", "25"))
EXECUTION_DATETIME = datetime.fromisoformat(
    os.getenv("EXECUTION_DATETIME", "2020-01-21T11:00:00")
)
FORCE_REPROCESS = strtobool(os.getenv("FORCE_REPROCESS", "False"))


"""
define schema according to https://wikitech.wikimedia.org/wiki/Analytics/Data_Lake/Traffic/Pageviews
#domain_code page_title count_views total_response_size
"""
data_schema = (
    StructType()
    .add("domain_code", StringType(), True)
    .add("page_title", StringType(), True)
    .add("count_views", LongType(), True)
    .add("total_response_size", LongType(), True)
)
blacklist_schema = (
    StructType()
    .add("domain_code", StringType(), True)
    .add("page_title", StringType(), True)
)


## Logging configs
logging_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
logging_datefmt = "%Y-%m-%d %H:%M:%S"
logging.basicConfig(format=logging_format, datefmt=logging_datefmt, level=logging.INFO)
logger = logging.getLogger(APP_NAME)
