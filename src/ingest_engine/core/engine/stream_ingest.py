import argparse
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col

from ingest_engine.core.utils.utils import add_ingest_metadata, get_logger
from ingest_engine.core.utils.config_loader import load_config
from ingest_engine.core.constants.constants import *

logger = get_logger("stream_ingest")

def run_stream_kafka(dataset_cfg: dict):
    spark = SparkSession.builder.getOrCreate()

    try:
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(spark)
    except ImportError:
        logger.warning("DBUtils no está disponible localmente.")
        dbutils = None

    kafka_cfg = dataset_cfg["kafka"]
    bronze = dataset_cfg["bronze_path"]
    checkpoint = dataset_cfg["checkpointLocation"]
    schema_path = dataset_cfg.get("schema")

    logger.info(f"Starting Kafka stream for {dataset_cfg['name']}")

    api_key = kafka_cfg.get("api_key")

    try:
        api_secret = dbutils.secrets.get(scope=SECRET_SCOPE, key=KAFKA_SECRET_KEY)
    except Exception as e:
        logger.error("No se pudo leer el secreto de Kafka del Key Vault.")
        raise e

    jaas_config = JAAS_TEMPLATE.format(api_key, api_secret)

    reader = (spark.readStream
              .format(FORMAT_KAFKA)
              .option("kafka.bootstrap.servers", kafka_cfg.get("bootstrapServers"))
              .option("subscribePattern", kafka_cfg.get("subscribePattern"))
              .option("startingOffsets", "earliest")
              .option("kafka.security.protocol", kafka_cfg.get("kafka.security.protocol"))
              .option("kafka.sasl.mechanism", kafka_cfg.get("kafka.sasl.mechanism"))
              .option("kafka.sasl.jaas.config", jaas_config))

    raw = reader.load()

    json_schema = None
    if schema_path:
        try:
            with open(schema_path, "r", encoding="utf-8") as f:
                json_schema = json.load(f)
        except Exception as e:
            logger.warning(f"No se pudo cargar el esquema: {e}")
            json_schema = None

    if json_schema:
        parsed = raw.select(from_json(col(COL_VALUE).cast("string"), json_schema).alias(COL_DATA)).select(f"{COL_DATA}.*")
    else:
        parsed = raw.selectExpr(f"CAST({COL_VALUE} AS STRING) as {COL_VALUE}")

    parsed2 = add_ingest_metadata(parsed, source_system=SOURCE_KAFKA)

    query = (parsed2.writeStream
             .format(FORMAT_DELTA)
             .option("checkpointLocation", checkpoint)
             .outputMode(MODE_APPEND)
             .start(bronze))

    logger.info("Kafka stream started and waiting for events...")
    query.awaitTermination()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    args = parser.parse_args()

    cfg = load_config(args.config)

    for ds in cfg.get("datasets", []):
        if ds.get("streaming", {}).get("enabled", False) and ds.get("type") == TYPE_KAFKA:
            logger.info(f"Running streaming ingest for dataset: {ds.get('name')}")
            run_stream_kafka(ds)

if __name__ == "__main__":
    main()