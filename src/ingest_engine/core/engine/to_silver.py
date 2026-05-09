# to_silver.py
import os, yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col, current_date
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, desc
from delta.tables import DeltaTable

CONFIG_PATH = os.environ.get("DATASET_CONFIG", "configs/dataset_config.yaml")

def load_config(path):
    with open(path, "r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)

def get_paths(cfg, name="sales"):
    for d in cfg.get("datasets", []):
        if d.get("name") == name:
            return d.get("bronze_path"), d.get("silver_path")
    raise RuntimeError("Dataset not found in config")

def main():
    spark = SparkSession.builder.getOrCreate()
    cfg = load_config(CONFIG_PATH)
    bronze_path, silver_path = get_paths(cfg, "sales")
    print("Bronze:", bronze_path, "Silver:", silver_path)

    df = spark.read.format("delta").load(bronze_path)

    df2 = (df.withColumn("order_date", to_date(col("order_date")))
           .withColumn("price", col("price").cast("double"))
           .withColumn("quantity", col("quantity").cast("int"))
           .withColumn("ingest_date", current_date()))

    w = Window.partitionBy("order_id").orderBy(desc("ingest_timestamp"))
    dedup = df2.withColumn("rn", row_number().over(w)).filter(col("rn") == 1).drop("rn")

    if DeltaTable.isDeltaTable(spark, silver_path):
        delta = DeltaTable.forPath(spark, silver_path)
        (delta.alias("t")
         .merge(dedup.alias("s"), "t.order_id = s.order_id")
         .whenMatchedUpdateAll()
         .whenNotMatchedInsertAll()
         .execute())
    else:
        dedup.write.format("delta").mode("overwrite").partitionBy("ingest_date").save(silver_path)
    print("Silver update finished")

if __name__ == "__main__":
    main()
