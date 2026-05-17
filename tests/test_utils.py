# tests/test_utils.py
from ingest_engine.core.utils.utils import add_ingest_metadata
from ingest_engine.core.constants.constants import COL_INGEST_TIMESTAMP, COL_SOURCE_SYSTEM, COL_SOURCE_FILE

def test_add_ingest_metadata_kafka(spark):
    df = spark.sql("SELECT 'dato1' as value UNION ALL SELECT 'dato2' as value")

    res_df = add_ingest_metadata(df, source_system="kafka")

    cols = res_df.columns
    assert "value" in cols
    assert COL_INGEST_TIMESTAMP in cols
    assert COL_SOURCE_SYSTEM in cols
    assert COL_SOURCE_FILE in cols

    row = res_df.first()
    assert row[COL_SOURCE_SYSTEM] == "kafka"
    assert row[COL_SOURCE_FILE] is None