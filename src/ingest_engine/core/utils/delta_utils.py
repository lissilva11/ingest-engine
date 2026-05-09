from delta.tables import DeltaTable
from pyspark.sql import DataFrame

def upsert_to_delta(df: DataFrame, target_path: str, key_cols: list):
    spark = df.sparkSession
    if DeltaTable.isDeltaTable(spark, target_path):
        delta = DeltaTable.forPath(spark, target_path)
        merge_condition = " AND ".join([f"target.{c} = source.{c}" for c in key_cols])
        (delta.alias("target")
         .merge(df.alias("source"), merge_condition)
         .whenMatchedUpdateAll()
         .whenNotMatchedInsertAll()
         .execute())
    else:
        df.write.format("delta").mode("overwrite").save(target_path)
