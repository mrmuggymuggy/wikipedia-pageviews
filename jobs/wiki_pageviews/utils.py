"""
tool sets for spark job
"""
import os, boto3, botocore
from urllib.parse import urlparse
from jobs.wiki_pageviews.wiki_pageviews_config import (
    EXECUTION_DATETIME,
    SOURCE_URL_PREFIX,
    PATH_OUT_PREFIX,
    HOURLY,
    logger,
)
from pyspark.sql import SparkSession


def add_pageviews_fileurls(spark: SparkSession):
    """
    Add pagesviews files to spark-context, two case are treated here
    1) Daily process
    2) Hourly process
    for daily process there are 24 files to add

    :param spark_session: spark session
    :return a filenames regex added to spark context, to be load as path later with spark.read
    >>> EXECUTION_DATETIME="2020-01-21T23:00:00"
        HOURLY=True
        will add to spark context : https://dumps.wikimedia.org/other/pageviews/2020/2020-10/pageviews-20201021-230000.gz
        return pageviews-20201021*.gz
        with
        EXECUTION_DATETIME="2020-01-21T23:00:00"
        HOURLY=False
        will each hour's url to spark context and
        return pageviews-20201021*.gz
    """
    #
    ts = EXECUTION_DATETIME
    # if not hourly then daily, add all files from the day
    if not HOURLY:
        for i in range(24):
            pageviews_filename = (
                f'{ts.strftime("%Y/%Y-%m/pageviews-%Y%m%d")}-{str(i).zfill(2)}0000.gz'
            )
            pageviews_file_url = f"{SOURCE_URL_PREFIX}/{pageviews_filename}"
            spark.sparkContext.addFile(pageviews_file_url)
    # default to process hourly data
    else:
        pageviews_filename = f'{ts.strftime("%Y/%Y-%m/pageviews-%Y%m%d-%H0000")}.gz'
        pageviews_file_url = f"{SOURCE_URL_PREFIX}/{pageviews_filename}"
        spark.sparkContext.addFile(pageviews_file_url)

    filenames_regex = ts.strftime("pageviews-%Y%m%d*.gz")
    return filenames_regex


def check_s3_key_exists(bucket, key):
    """
    check if s3 key exists in bucket
    used to check if data is been processed, spark job write _SUCCESS file when job is done
    :param bucket: s3 bucker
    :param key: s3 key
    :return if key exists in s3 bucket
    """
    s3 = boto3.resource("s3")
    try:
        # load() does a HEAD request for a single key, it's fast
        s3.Object(bucket, key).load()
    except botocore.exceptions.ClientError as e:
        logger.info(f"Can't find {key} from {bucket}, error code : {e}")
        return False
    return True


def pageviews_are_processed():
    """
    Check if output path contains _SUCCESS file
    Check for remote or local
    """
    partitioned_out_path = get_partitioned_output_path()
    url = urlparse(partitioned_out_path)
    if url.scheme in ["s3", "s3a"]:
        return check_s3_key_exists(url.netloc, f"{url.path.lstrip('/')}/_SUCCESS")
    else:
        return os.path.isfile(f"{partitioned_out_path}/_SUCCESS")


def get_partitioned_output_path():
    """
    Get the partitioned output patch string
    :return year/month/day/hour partitioned output path

    >>> EXECUTION_DATETIME="2020-01-21T23:00:00"
        HOURLY=True
        partitioned_out_path = {PATH_OUT_PREFIX}format=csv/year=2020/month=01/day=21/hour=23
        with
        EXECUTION_DATETIME="2020-01-21T23:00:00"
        HOURLY=False
        partitioned_out_path = {PATH_OUT_PREFIX}format=csv/year=2020/month=01/day=21

    """
    ts = EXECUTION_DATETIME
    if not HOURLY:
        partitioned_out_path = (
            f'{PATH_OUT_PREFIX}/format=csv/{ts.strftime("year=%Y/month=%m/day=%d")}'
        )
    else:
        partitioned_out_path = f'{PATH_OUT_PREFIX}/format=csv/{ts.strftime("year=%Y/month=%m/day=%d/hour=%H")}'
    return partitioned_out_path
