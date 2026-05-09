import argparse
from core.engine.config_loader import load_config
from core.engine.batch_ingest import run_batch
from core.engine.stream_ingest import run_stream_kafka
from core.utils.utils import get_logger

logger = get_logger()

def process_dataset(ds: dict):
    if ds.get("type") == "file" and ds.get("batch", {}).get("enabled", False):
        run_batch(ds)
    if ds.get("streaming", {}).get("enabled", False) or ds.get("streaming", {}).get("enabled", False) is True:
        run_stream_kafka(ds)

def main(config_path: str):
    cfg = load_config(config_path)
    datasets = cfg.get("datasets", [])
    for ds in datasets:
        logger.info(f"Processing dataset {ds.get('name')}")
        process_dataset(ds)

def cli_entry():
    parser = argparse.ArgumentParser(description="Ingest Engine")
    parser.add_argument("--config", required=True, help="Path to dataset_config.yaml")
    args = parser.parse_args()
    main(args.config)

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python -m ingest_engine.main --config path/to/dataset_config.yaml")
    else:
        cli_entry()