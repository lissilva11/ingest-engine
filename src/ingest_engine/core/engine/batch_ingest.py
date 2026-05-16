import argparse
from pyspark.sql import SparkSession

from ingest_engine.core.utils.utils import add_ingest_metadata, get_logger
from ingest_engine.core.utils.config_loader import load_config
from ingest_engine.core.constants.constants import *

logger = get_logger("batch_ingest")

def run_batch(dataset_cfg: dict):
    spark = SparkSession.builder.getOrCreate()
    landing = dataset_cfg["landing_path"]
    bronze = dataset_cfg["bronze_path"]
    fmt = dataset_cfg.get("format", FORMAT_JSON)
    schema_loc = dataset_cfg["autoloader"]["schemaLocation"]
    checkpoint = dataset_cfg["autoloader"]["checkpointLocation"]

    logger.info(f"Starting Autoloader for {dataset_cfg['name']} from {landing} to {bronze}")

    df = (spark.readStream
          .format(FORMAT_CLOUD_FILES)
          .option("cloudFiles.format", fmt)
          .option("cloudFiles.schemaLocation", schema_loc)
          .load(landing))

    df2 = add_ingest_metadata(df, source_system=SOURCE_FILE)

    query = (df2.writeStream
             .format(FORMAT_DELTA)
             .option("checkpointLocation", checkpoint)
             .outputMode(MODE_APPEND)
             .trigger(availableNow=True)
             .start(bronze))

    logger.info("Autoloader started, waiting for completion")
    query.awaitTermination()
    logger.info("Autoloader finished")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    args = parser.parse_args()

    cfg = load_config(args.config)

    for ds in cfg.get("datasets", []):
        if ds.get("batch", {}).get("enabled", False):
            logger.info(f"Running batch ingest for dataset: {ds.get('name')}")
            run_batch(ds)

if __name__ == "__main__":
    main()