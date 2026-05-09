from pyspark.sql import SparkSession
from core.utils.utils import add_ingest_metadata, get_logger

logger = get_logger("batch_ingest")

def run_batch(dataset_cfg: dict):
    spark = SparkSession.builder.getOrCreate()
    landing = dataset_cfg["landing_path"]
    bronze = dataset_cfg["bronze_path"]
    fmt = dataset_cfg.get("format", "json")
    schema_loc = dataset_cfg["autoloader"]["schemaLocation"]
    checkpoint = dataset_cfg["autoloader"]["checkpointLocation"]

    logger.info(f"Starting Autoloader for {dataset_cfg['name']} from {landing} to {bronze}")

    df = (spark.readStream
          .format("cloudFiles")
          .option("cloudFiles.format", fmt)
          .option("cloudFiles.schemaLocation", schema_loc)
          .load(landing))

    df2 = add_ingest_metadata(df, source_system="file")

    query = (df2.writeStream
             .format("delta")
             .option("checkpointLocation", checkpoint)
             .outputMode("append")
             .trigger(availableNow=True)
             .start(bronze))

    logger.info("Autoloader started, waiting for completion")
    query.awaitTermination()
    logger.info("Autoloader finished")
