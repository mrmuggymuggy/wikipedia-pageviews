"""
ETL python job for wikipedia pageviews top ranks
"""
import sys
from typing import Tuple
from pyspark.sql import DataFrame, SparkSession
from pyspark import SparkFiles

from jobs.wiki_pageviews.wiki_pageviews_config import (
    APP_NAME,
    SOURCE_URL_PREFIX,
    BLACKLIST_URL_IN,
    PATH_OUT_PREFIX,
    EXECUTION_DATETIME,
    FORCE_REPROCESS,
    data_schema,
    blacklist_schema,
    logger,
)
from jobs.wiki_pageviews.wiki_pageviews_model import (
    data_clean,
    aggregate_pageviews,
    compute_ranks,
)
from jobs.wiki_pageviews.utils import (
    get_partitioned_output_path,
    pageviews_are_processed,
    add_pageviews_fileurls,
)


def main():
    data_is_processed = pageviews_are_processed()
    if data_is_processed and (not FORCE_REPROCESS):
        logger.info(
            f"pageviews at {EXECUTION_DATETIME} have been processed and FORCE_REPROCESS flag is off"
        )
        sys.exit(0)

    spark = SparkSession.builder.appName(APP_NAME).getOrCreate()
    data_tuple = extract_data(spark)
    data_transformed = transform_data(spark, data_tuple)
    load_data(data_transformed)
    logger.info(
        f"""Starting batch query to compute wikipedia pageview ranks
        with {SOURCE_URL_PREFIX} as source
        and {PATH_OUT_PREFIX} as output path."""
    )
    spark.stop()


def extract_data(spark: SparkSession) -> Tuple[DataFrame, DataFrame]:
    """
    SPARK ETL --> E as Extract
    Extract pageviews data and blacklist data from remote source(http) with csv format
    Load them as Spark Dataframes

    :param spark_session: spark session
    :return: a Tuple of dataframes -> pageviews dataframe and blacklist dataframe
    """
    spark.sparkContext.addFile(BLACKLIST_URL_IN)
    blacklist_filename = BLACKLIST_URL_IN.split("/")[-1]
    pageviews_filenames_regex = add_pageviews_fileurls(spark)

    blacklist_df = (
        spark.read.option("delimiter", " ")
        .option("inferSchema", True)
        .schema(blacklist_schema)
        .csv(f"file://{SparkFiles.get(blacklist_filename)}")
    )
    data_df = (
        spark.read.option("delimiter", " ")
        .option("inferSchema", True)
        .schema(data_schema)
        .csv(f"file://{SparkFiles.get(pageviews_filenames_regex)}")
    )

    return (data_df, blacklist_df)


def transform_data(spark_session, data: Tuple[DataFrame, DataFrame]) -> DataFrame:
    """
    SPARK ETL --> T as Transform
    1) Clean pageviews dataframe with blacklist dataframe
    2) Aggregate cleaned data with by page_titles and domain_code
    3) Compute topranks woth aggregated clean data

    :param spark_session: spark session
    :param data: Tuple of dataframes -> pageviews dataframe and blacklist dataframe
    :return: A dataframe contains top ranked page_titles per domain_code
    """
    data_df, blacklist_df = data
    cleaned_df = data_clean(spark_session, data_df, blacklist_df)
    aggregated_df = aggregate_pageviews(spark_session, cleaned_df)
    topranked_df = compute_ranks(spark_session, aggregated_df)
    return topranked_df


def load_data(data: DataFrame) -> None:
    """
    SPARK ETL --> L as Load
    Load final top ranked dataframe to different targets, support s3 or local
    default gziped csv format

    output path format: outputs/format=csv/year=2020/month=01/day=21/hour=12

    :param data: a dataframe contains top ranked page_titles by domain_code
    :return: None
    """
    partitioned_out_path = get_partitioned_output_path()
    data.repartition(1).write.option("compression", "gzip").option(
        "header", True
    ).option("inferSchema", True).option("delimiter", ",").save(
        path=partitioned_out_path, format="csv", mode="overwrite",
    )


# entry point for PySpark ETL application
if __name__ == "__main__":
    main()
