# producer_to_landing.py
import time, json, random, os
from datetime import datetime
from pathlib import Path
import yaml

# Config
CONFIG_PATH = os.environ.get("DATASET_CONFIG", "configs/dataset_config.yaml")
OUT_DIR = os.environ.get("PRODUCER_OUT_DIR", "./landing/sales")  # default local
BATCH_SIZE = int(os.environ.get("PRODUCER_BATCH_SIZE", "50"))
SLEEP_SECONDS = int(os.environ.get("PRODUCER_SLEEP_SECONDS", "30"))

def load_config(path):
    with open(path, "r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)

def get_landing_path(cfg, dataset_name="sales"):
    for d in cfg.get("datasets", []):
        if d.get("name") == dataset_name:
            return d.get("landing_path")
    return None

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
    Path(OUT_DIR).mkdir(parents=True, exist_ok=True)
    cfg = load_config(CONFIG_PATH)
    landing_path = get_landing_path(cfg, "sales")
    print("Using landing_path from config:", landing_path)
    i = 0
    while True:
        rows = [make_order(i + j) for j in range(BATCH_SIZE)]
        fname = f"{OUT_DIR}/sales_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}_{i}.json"
        with open(fname, "w", encoding="utf-8") as fh:
            for r in rows:
                fh.write(json.dumps(r, ensure_ascii=False) + "\n")
        print(f"Wrote {fname} ({len(rows)} records)")
        i += BATCH_SIZE
        time.sleep(SLEEP_SECONDS)

if __name__ == "__main__":
    main()
