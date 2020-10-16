"""
ETL python for creating master data from S3 to S3
"""
from python.wiki_pageviews.wiki_pageviews_config import (
    APP_NAME,
    SOURCE_URL_IN,
    BLACKLISG_URL_IN,
    PATH_OUT,
    data_schema,
    blacklist_schema,
    spark_session,
    logger,
)
from python.wiki_pageviews.wiki_pageviews_model import transform_wiki_pageviews
from typing import Tuple
from pyspark.sql import DataFrame, SparkSession
from pyspark import SparkFiles


def main():
    spark = spark_session
    data_tuple = extract_data(spark)
    data_transformed = transform_data(data_tuple)
    load_data(data_transformed)

    logger.info(
        f"Starting batch query to compute wikipedia pageview ranks with {SOURCE_URL_IN} as source and {PATH_OUT} as output path."
    )
    spark.stop()


"""
return tuple of data dataframe and blacklist dataframe
"""


def extract_data(spark: SparkSession) -> Tuple[DataFrame, DataFrame]:
    spark.sparkContext.addFile(SOURCE_URL_IN)
    spark.sparkContext.addFile(BLACKLISG_URL_IN)
    data_filename = SOURCE_URL_IN.split("/")[-1]
    blacklist_filename = BLACKLISG_URL_IN.split("/")[-1]

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
        .csv(f"file://{SparkFiles.get(data_filename)}")
    )

    return (data_df, blacklist_df)


def transform_data(data: Tuple[DataFrame, DataFrame]) -> DataFrame:
    data = transform_wiki_pageviews(data)
    return data


def load_data(data: DataFrame) -> None:
    data.repartition(1).write.option("header", True).option("inferSchema", True).option(
        "delimiter", ","
    ).mode("overwrite").csv(PATH_OUT)


# entry point for PySpark ETL application
if __name__ == "__main__":
    main()
