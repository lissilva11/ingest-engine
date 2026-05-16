import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col, sum as _sum

from ingest_engine.core.utils.config_loader import load_config, get_paths
from ingest_engine.core.constants.constants import *

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()
    cfg = load_config(args.config)
    _, silver_path = get_paths(cfg, DATASET_SALES)
    print("Silver:", silver_path, "Gold: Managed by Unity Catalog")

    df = spark.read.format(FORMAT_DELTA).load(silver_path)

    agg = (df.groupBy(to_date(col("order_date")).alias("order_day"), col("region"))
    .agg(
        _sum(col("quantity") * col("price")).alias("total_revenue"),
        _sum(col("quantity")).alias("total_units")
    ))

    # Usamos las constantes del esquema para guardar en Unity Catalog
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_GOLD}")
    agg.write.format(FORMAT_DELTA).mode(MODE_OVERWRITE).partitionBy("order_day").saveAsTable(TABLE_GOLD_SALES)

    print(f"Gold tables updated in Unity Catalog: {TABLE_GOLD_SALES}!")

if __name__ == "__main__":
    main()