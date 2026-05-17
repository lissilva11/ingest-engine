import sys
import os
import pytest
from pathlib import Path
from pyspark.sql import SparkSession

ROOT = Path(__file__).resolve().parents[1]  # repo root
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

@pytest.fixture(scope="session")
def spark():
    """Crea una sesión de Spark en local solo para ejecutar los tests."""
    spark = SparkSession.builder \
        .master("local[1]") \
        .appName("local-testing") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.sql.shuffle.partitions", "1") \
        .getOrCreate()
    yield spark
    spark.stop()