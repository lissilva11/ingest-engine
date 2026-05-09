from pyspark.sql import SparkSession
from core.utils.utils import add_ingest_metadata, get_logger
from pyspark.sql.functions import from_json, col
import json

logger = get_logger("stream_ingest")

def run_stream_kafka(dataset_cfg: dict):
    spark = SparkSession.builder.getOrCreate()
    kafka_cfg = dataset_cfg["kafka"]
    bronze = dataset_cfg["bronze_path"]
    checkpoint = dataset_cfg["checkpointLocation"]
    schema_path = dataset_cfg.get("schema")

    logger.info(f"Starting Kafka stream for {dataset_cfg['name']}")

    raw = (spark.readStream
           .format("kafka")
           .option("kafka.bootstrap.servers", kafka_cfg["bootstrapServers"])
           .option("subscribePattern", kafka_cfg.get("subscribePattern"))
           .option("startingOffsets", "latest")
           .load())

    json_schema = None
    if schema_path:
        # read a single JSON to infer schema or load schema definition
        try:
            with open(schema_path, "r", encoding="utf-8") as f:
                schema_json = json.load(f)
            # If you want to convert to Spark schema, do it in runtime; here we parse as string
        except Exception:
            json_schema = None

    parsed = raw.select(from_json(col("value").cast("string"), json_schema).alias("data")).select("data.*") if json_schema else raw.selectExpr("CAST(value AS STRING) as value")

    parsed2 = add_ingest_metadata(parsed, source_system="kafka")

    query = (parsed2.writeStream
             .format("delta")
             .option("checkpointLocation", checkpoint)
             .outputMode("append")
             .start(bronze))

    logger.info("Kafka stream started")
    query.awaitTermination()
