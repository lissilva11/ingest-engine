# ingest_engine/core/utils/utils.py
import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp, lit, col
from ingest_engine.core.constants.constants import *

def get_logger(name: str = "ingest_engine"):
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger

def add_ingest_metadata(df: DataFrame, source_system: str):
    df_with_meta = (df.withColumn(COL_INGEST_TIMESTAMP, current_timestamp())
                    .withColumn(COL_SOURCE_SYSTEM, lit(source_system)))

    if source_system == SOURCE_FILE:
        return df_with_meta.withColumn(COL_SOURCE_FILE, col("_metadata.file_path"))
    else:
        return df_with_meta.withColumn(COL_SOURCE_FILE, lit(None).cast("string"))