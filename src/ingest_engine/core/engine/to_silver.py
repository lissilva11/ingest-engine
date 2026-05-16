import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col, current_date
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, desc
from delta.tables import DeltaTable

from ingest_engine.core.utils.config_loader import load_config, get_paths
from ingest_engine.core.constants.constants import *

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()
    cfg = load_config(args.config)
    bronze_path, silver_path = get_paths(cfg, DATASET_SALES)
    print("Bronze:", bronze_path, "Silver:", silver_path)

    df = spark.read.format(FORMAT_DELTA).load(bronze_path)

    df2 = (df.withColumn("order_date", to_date(col("order_date")))
           .withColumn("price", col("price").cast("double"))
           .withColumn("quantity", col("quantity").cast("int"))
           .withColumn("ingest_date", current_date()))

    w = Window.partitionBy("order_id").orderBy(desc(COL_INGEST_TIMESTAMP))
    dedup = df2.withColumn("rn", row_number().over(w)).filter(col("rn") == 1).drop("rn")

    if DeltaTable.isDeltaTable(spark, silver_path):
        delta = DeltaTable.forPath(spark, silver_path)
        (delta.alias("t")
         .merge(dedup.alias("s"), "t.order_id = s.order_id")
         .whenMatchedUpdateAll()
         .whenNotMatchedInsertAll()
         .execute())
    else:
        dedup.write.format(FORMAT_DELTA).mode(MODE_OVERWRITE).partitionBy("ingest_date").save(silver_path)
    print("Silver update finished")

if __name__ == "__main__":
    main()