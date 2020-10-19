"""
ETL functions for wikipedia pageviews
"""
from pyspark.sql.dataframe import DataFrame
from typing import Tuple
from jobs.wiki_pageviews.wiki_pageviews_config import logger


def data_clean(spark_session, data_df, blacklist_df) -> DataFrame:
    """
    Remove blacklisted domain_code and page_title pairs from original pageview dataframes

    :param spark_session: spark session
    :param data_df: pageviews dataframe.
    :param blacklist_df: blacklist domain_code and page_title pairs dataframe.
    :return a cleaned dataframe without blacklisted entries
    """
    data_df.createOrReplaceTempView("wiki_pageviews")
    blacklist_df.createOrReplaceTempView("blacklists")
    # better query is possible, we can check spark execute plan
    filter_query = """
    SELECT *
    FROM   wiki_pageviews w
    WHERE NOT EXISTS (
       SELECT *
       FROM   blacklists
       WHERE  domain_code = w.domain_code and page_title = w.page_title
       );
    """
    logger.info(f"data clean with query : {filter_query}")
    cleaned_df = spark_session.sql(filter_query)
    return cleaned_df


def aggregate_pageviews(spark_session, cleaned_df) -> DataFrame:
    """
    Aggregate pageviews dataframe by domain_code and page_title, it is needed when inputs
    more than one hour data

    :param spark_session: spark session
    :param cleaned_df: cleaned dataframe without blacklisted entries.
    :return: a dataframe aggregated by domain_code and page_title
    """
    cleaned_df.createOrReplaceTempView("cleaned_wiki_pageviews")
    aggregate_query = """
    SELECT  domain_code,
            page_title,
            Sum(count_views) as count_views
    FROM cleaned_wiki_pageviews
    GROUP BY domain_code, page_title;
    """
    logger.info(f"data aggregation with query : {aggregate_query}")
    aggregated_df = spark_session.sql(aggregate_query)
    return aggregated_df


def compute_ranks(spark_session, aggregated_df) -> DataFrame:
    """
    Compute topranks with aggregated and cleaned dataframe

    :param spark_session: spark session
    :param aggregated_df: aggregated cleaned dataframe.
    :return: a dataframe contains top ranked page_titles by domain_code
    """
    aggregated_df.createOrReplaceTempView("aggregated_wiki_pageviews")
    """
    for sql knowledge :)
    http://www.silota.com/docs/recipes/sql-top-n-group.html
    #maybe use dense_rank or rank
    """
    rank_query = """
    SELECT * FROM (
        SELECT domain_code,
               count_views,
               page_title,
               dense_rank() over (partition by domain_code ORDER BY count_views DESC) AS domain_rank
        FROM aggregated_wiki_pageviews) ranks
    WHERE domain_rank <= 3;
    """
    logger.info(f"compute pageviews rank with query : {rank_query}")
    ranked_df = spark_session.sql(rank_query)
    return ranked_df
