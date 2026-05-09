import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp, input_file_name, lit

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
    return (df.withColumn("ingest_timestamp", current_timestamp())
            .withColumn("source_system", lit(source_system))
            .withColumn("source_file", input_file_name()))
