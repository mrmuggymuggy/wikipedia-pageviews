import os, sys, logging
from pyspark.sql.types import LongType, StringType, StructType

APP_NAME = os.getenv("APP_NAME", "wiki_pageviews")
SOURCE_URL_IN = os.getenv("SOURCE_URL_IN", "to_be_set_in_run_time")
BLACKLISG_URL_IN = os.getenv("BLACKLISG_URL_IN", "to_be_set_in_run_time")
PATH_OUT = os.getenv("PATH_OUT", "to_be_set_in_run_time")
# TOP_RANK = ensure_env("TOP_RANK")

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

toprank_schema = (
    StructType()
    .add("domain_code", StringType(), True)
    .add("count_views", LongType(), True)
    .add("page_title", StringType(), True)
    .add("domain_rank", LongType(), True)
)

logger = logging.getLogger(APP_NAME)
