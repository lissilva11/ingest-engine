# to_gold.py
import os, yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col, sum as _sum

CONFIG_PATH = os.environ.get("DATASET_CONFIG", "configs/dataset_config.yaml")

def load_config(path):
    with open(path, "r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)

def get_paths(cfg, name="sales"):
    for d in cfg.get("datasets", []):
        if d.get("name") == name:
            return d.get("silver_path"), d.get("gold_path")
    raise RuntimeError("Dataset not found in config")

def main():
    spark = SparkSession.builder.getOrCreate()
    cfg = load_config(CONFIG_PATH)
    silver_path, gold_path = get_paths(cfg, "sales")
    print("Silver:", silver_path, "Gold:", gold_path)

    df = spark.read.format("delta").load(silver_path)

    agg = (df.groupBy(to_date(col("order_date")).alias("order_day"), col("region"))
    .agg(
        _sum(col("quantity") * col("price")).alias("total_revenue"),
        _sum(col("quantity")).alias("total_units")
    ))

    agg.write.format("delta").mode("overwrite").partitionBy("order_day").save(gold_path)
    spark.sql(f"CREATE TABLE IF NOT EXISTS gold.sales_daily USING DELTA LOCATION '{gold_path}'")
    print("Gold tables updated")

if __name__ == "__main__":
    main()
