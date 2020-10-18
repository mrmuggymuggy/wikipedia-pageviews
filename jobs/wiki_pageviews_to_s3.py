"""
ETL python for creating master data from S3 to S3
"""
from jobs.wiki_pageviews.wiki_pageviews_config import (
    APP_NAME,
    SOURCE_URL_PREFIX,
    BLACKLIST_URL_IN,
    PATH_OUT,
    HOURLY,
    EXECUTION_DATETIME,
    TOP_RANK,
    data_schema,
    blacklist_schema,
    logger,
)
from jobs.wiki_pageviews.wiki_pageviews_model import transform_wiki_pageviews
from typing import Tuple
from pyspark.sql import DataFrame, SparkSession
from pyspark import SparkFiles


def main():
    spark = SparkSession.builder.appName(APP_NAME).getOrCreate()
    data_tuple = extract_data(spark)
    data_transformed = transform_data(spark, data_tuple)
    load_data(data_transformed)
    logger.info(
        f"Starting batch query to compute wikipedia pageview ranks with {SOURCE_URL_PREFIX} as source and {PATH_OUT} as output path."
    )
    spark.stop()


def _extract_pageview_files(spark: SparkSession) -> DataFrame:
    # "SOURCE_URL_PREFIX": "https://dumps.wikimedia.org/other/pageviews"
    # https://dumps.wikimedia.org/other/pageviews/2020/2020-10/pageviews-20201001-020000.gz
    ts = EXECUTION_DATETIME
    # if not hourly then daily, add all files from the day
    if HOURLY == "False":
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
    data_df = (
        spark.read.option("delimiter", " ")
        .option("inferSchema", True)
        .schema(data_schema)
        .csv(f"file://{SparkFiles.get(filenames_regex)}")
    )
    return data_df


"""
return tuple of data dataframe and blacklist dataframe
"""


def extract_data(spark: SparkSession) -> Tuple[DataFrame, DataFrame]:
    spark.sparkContext.addFile(BLACKLIST_URL_IN)
    blacklist_filename = BLACKLIST_URL_IN.split("/")[-1]
    blacklist_df = (
        spark.read.option("delimiter", " ")
        .option("inferSchema", True)
        .schema(blacklist_schema)
        .csv(f"file://{SparkFiles.get(blacklist_filename)}")
    )

    data_df = _extract_pageview_files(spark)

    return (data_df, blacklist_df)


def transform_data(spark_session, data: Tuple[DataFrame, DataFrame]) -> DataFrame:
    data = transform_wiki_pageviews(spark_session, data)
    return data


def load_data(data: DataFrame) -> None:
    ts = EXECUTION_DATETIME
    if HOURLY == "False":
        partitioned_out_path = (
            f'{PATH_OUT}/format=csv/{ts.strftime("year=%Y/month=%m/day=%d")}'
        )
    else:
        partitioned_out_path = (
            f'{PATH_OUT}/format=csv/{ts.strftime("year=%Y/month=%m/day=%d/hour=%H")}'
        )

    data.repartition(1).write.option("delimiter", ",").option("header", True).option(
        "inferSchema", True
    ).mode("overwrite").csv(partitioned_out_path)


# entry point for PySpark ETL application
if __name__ == "__main__":
    main()
