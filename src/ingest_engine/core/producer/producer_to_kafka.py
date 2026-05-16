import time, random, argparse
from datetime import datetime
from pyspark.sql import SparkSession

from ingest_engine.core.utils.config_loader import load_config
from ingest_engine.core.constants.constants import *

def make_iot_event(i):
    return {
        "sensor_id": f"SENS-{random.randint(1,100):03d}",
        "temperature": round(random.uniform(15.0, 35.0), 2),
        "humidity": round(random.uniform(40.0, 80.0), 2),
        "timestamp": datetime.utcnow().isoformat()
    }

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    args = parser.parse_args()

    cfg = load_config(args.config)

    kafka_cfg = next((d.get("kafka") for d in cfg.get("datasets", []) if d.get("name") == DATASET_EVENTS), None)
    topic = kafka_cfg.get("subscribePattern")

    spark = SparkSession.builder.getOrCreate()

    try:
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(spark)
    except ImportError:
        print("DBUtils no está disponible localmente.")
        dbutils = None

    api_key = kafka_cfg.get("api_key")
    try:
        api_secret = dbutils.secrets.get(scope=SECRET_SCOPE, key=KAFKA_SECRET_KEY)
    except Exception as e:
        print("Error: No se pudo leer el secreto de Kafka del Key Vault.")
        raise e

    jaas_config = JAAS_TEMPLATE.format(api_key, api_secret)

    options = {
        "kafka.bootstrap.servers": kafka_cfg.get("bootstrapServers"),
        "kafka.security.protocol": kafka_cfg.get("kafka.security.protocol"),
        "kafka.sasl.mechanism": kafka_cfg.get("kafka.sasl.mechanism"),
        "kafka.sasl.jaas.config": jaas_config
    }

    print(f"Iniciando simulador IoT hacia Kafka topic: {topic}")

    for i in range(15):
        events = [make_iot_event(i * 10 + j) for j in range(20)]

        df = spark.createDataFrame(events)
        df_kafka = df.selectExpr(f"to_json(struct(*)) AS {COL_VALUE}")

        df_kafka.write.format(FORMAT_KAFKA).option("topic", topic).options(**options).save()
        print(f"Enviado lote {i+1}/15 a Confluent Cloud")
        time.sleep(5)

    print("Producer IoT finalizado con éxito.")

if __name__ == "__main__":
    main()