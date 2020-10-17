from pyspark.sql.dataframe import DataFrame
from typing import Tuple
from jobs.wiki_pageviews.wiki_pageviews_config import logger


def data_clean(spark_session, data_df, blacklist_df) -> DataFrame:
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


def compute_ranks(spark_session, cleaned_df) -> DataFrame:
    cleaned_df.createOrReplaceTempView("cleaned_wiki_pageviews")
    """
    for sql knowledge :)
    http://www.silota.com/docs/recipes/sql-top-n-group.html
    #maybe use dense_rank or rank
    """
    rank_query = """
    select * from (
        select domain_code,
               count_views,
               page_title,
               dense_rank() over (partition by domain_code order by count_views desc) as domain_rank
        from cleaned_wiki_pageviews) ranks
    where domain_rank <= 3;
    """
    logger.info(f"compute pageviews rank with query : {rank_query}")
    ranked_df = spark_session.sql(rank_query)
    return ranked_df


def transform_wiki_pageviews(
    spark_session, data: Tuple[DataFrame, DataFrame]
) -> DataFrame:
    data_df, blacklist_df = data
    cleaned_df = data_clean(spark_session, data_df, blacklist_df)
    return compute_ranks(spark_session, cleaned_df)
