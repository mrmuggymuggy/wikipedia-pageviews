import os, sys
from pyspark.sql.types import LongType, StringType, StructType
from pyspark.sql import SparkSession

# small helper to exit program is ENV is not set
def ensure_env(ENV, default_val=None):
    val = os.getenv(ENV, default_val)
    if val is None:
        logger.info(f"{ENV} Is not set, exit program")
        sys.exit(1)
    return val


APP_NAME = ensure_env("APP_NAME")
SOURCE_URL_IN = ensure_env("SOURCE_URL_IN")
BLACKLISG_URL_IN = ensure_env("BLACKLISG_URL_IN")
PATH_OUT = ensure_env("PATH_OUT")
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

spark_session = SparkSession.builder.appName(APP_NAME).getOrCreate()
# Using Spark log4j logging
logger = spark_session.sparkContext._jvm.org.apache.log4j.LogManager.getLogger(APP_NAME)
