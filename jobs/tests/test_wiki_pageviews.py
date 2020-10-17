import pandas as pd

from jobs.wiki_pageviews.wiki_pageviews_model import (
    compute_ranks,
    data_clean,
)
from jobs.wiki_pageviews.wiki_pageviews_config import (
    data_schema,
    blacklist_schema,
    toprank_schema,
)


def test_data_clean(spark_session):
    """ test the availability/passing of different contexts """
    data = [
        ("fr.m", "Relations_entre_Israël_et_la_Syrie", 10, 0),
        ("fr.m", "Relations_entre_judaïsme_et_christianisme", 2, 0),
        ("fr.m", "Relations_entre_juifs_et_musulmans", 16, 0),
        ("fr.m", "Relations_entre_l'Algérie_et_le_Maroc", 10, 0),
        ("fr.m", "Relations_entre_l'Allemagne_et_l'Azerbaïdjan", 1, 0),
        ("de.m", "Wilde_Jagd", 5, 0),
        ("de.m", "Wilde_Malve", 5, 0),
        ("de.m", "Wilde_Müllkippe", 1, 0),
        ("de.m", "Wilde_Wasser", 1, 0),
        ("de.m", "Wildecker_Herzbuben", 9, 0),
    ]
    blacklist_data = [
        ("fr.m", "Relations_entre_l'Allemagne_et_l'Azerbaïdjan"),
        ("de.m", "Wilde_Jagd"),
    ]

    expected_result = [
        ("fr.m", "Relations_entre_Israël_et_la_Syrie", 10, 0),
        ("fr.m", "Relations_entre_judaïsme_et_christianisme", 2, 0),
        ("fr.m", "Relations_entre_juifs_et_musulmans", 16, 0),
        ("fr.m", "Relations_entre_l'Algérie_et_le_Maroc", 10, 0),
        ("de.m", "Wilde_Malve", 5, 0),
        ("de.m", "Wilde_Müllkippe", 1, 0),
        ("de.m", "Wilde_Wasser", 1, 0),
        ("de.m", "Wildecker_Herzbuben", 9, 0),
    ]

    df_data = spark_session.createDataFrame(data, schema=data_schema)
    df_blacklists = spark_session.createDataFrame(
        blacklist_data, schema=blacklist_schema
    )
    # nested columns need to be read as JSON, therefore toJSON()
    result_df = data_clean(spark_session, df_data, df_blacklists).toPandas()
    expected_result_df = spark_session.createDataFrame(
        expected_result, schema=data_schema
    ).toPandas()
    col_names = df_data.schema.names
    pd.testing.assert_frame_equal(
        result_df.set_index(col_names),
        expected_result_df.set_index(col_names),
        check_like=True,
        check_dtype=False,
    )


def test_compute_ranks(spark_session):
    """ test the availability/passing of different contexts """
    data = [
        ("fr.m", "Relations_entre_Israël_et_la_Syrie", 10, 0),
        ("fr.m", "Relations_entre_judaïsme_et_christianisme", 2, 0),
        ("fr.m", "Relations_entre_juifs_et_musulmans", 16, 0),
        ("fr.m", "Relations_entre_l'Algérie_et_le_Maroc", 10, 0),
        ("de.m", "Wilde_Malve", 5, 0),
        ("de.m", "Wilde_Müllkippe", 1, 0),
        ("de.m", "Wilde_Wasser", 1, 0),
        ("de.m", "Wildecker_Herzbuben", 9, 0),
    ]

    expected_result = [
        ("de.m", 9, "Wildecker_Herzbuben", 1),
        ("de.m", 5, "Wilde_Malve", 2),
        ("de.m", 1, "Wilde_Müllkippe", 3),
        ("de.m", 1, "Wilde_Wasser", 3),
        ("fr.m", 16, "Relations_entre_juifs_et_musulmans", 1),
        ("fr.m", 10, "Relations_entre_Israël_et_la_Syrie", 2),
        ("fr.m", 10, "Relations_entre_l'Algérie_et_le_Maroc", 2),
        ("fr.m", 2, "Relations_entre_judaïsme_et_christianisme", 3),
    ]

    df_data = spark_session.createDataFrame(data, schema=data_schema)
    result_df = compute_ranks(spark_session, df_data).toPandas()
    expected_result_df = spark_session.createDataFrame(
        expected_result, schema=toprank_schema
    ).toPandas()

    pd.testing.assert_frame_equal(
        result_df, expected_result_df, check_like=True, check_dtype=False
    )
