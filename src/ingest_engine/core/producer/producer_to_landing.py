import time, random, argparse
from datetime import datetime
from pyspark.sql import SparkSession

from ingest_engine.core.utils.config_loader import load_config
from ingest_engine.core.constants.constants import *

def make_order(i):
    products = ["P001","P002","P003","P004"]
    regions = ["ES","FR","DE","IT"]
    return {
        "order_id": f"ORD-{i:08d}",
        "customer_id": f"CUST-{random.randint(1,2000):05d}",
        "product_id": random.choice(products),
        "quantity": random.randint(1,10),
        "price": round(random.uniform(5,200),2),
        "order_date": datetime.utcnow().isoformat(),
        "region": random.choice(regions)
    }

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True, help="Path to dataset_config.yaml")
    args = parser.parse_args()

    cfg = load_config(args.config)

    landing_path = next((d.get("landing_path") for d in cfg.get("datasets", []) if d.get("name") == DATASET_SALES), None)
    print("Writing directly to Azure:", landing_path)

    spark = SparkSession.builder.getOrCreate()

    batch_size = 50
    sleep_seconds = 30
    i = 0
    max_iterations = 5

    for _ in range(max_iterations):
        rows = [make_order(i + j) for j in range(batch_size)]

        df = spark.createDataFrame(rows)
        df.coalesce(1).write.format(FORMAT_JSON).mode(MODE_APPEND).save(landing_path)

        print(f"Wrote batch of {len(rows)} records to {landing_path}")
        i += batch_size
        time.sleep(sleep_seconds)

if __name__ == "__main__":
    main()