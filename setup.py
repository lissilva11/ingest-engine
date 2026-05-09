from setuptools import setup, find_packages

setup(
    name="ingest_engine",
    version="0.1.0",
    description="Motor de ingesta lakehouse (landing -> bronze -> silver -> gold) para Databricks",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "pyyaml>=5.4",
        "pyspark>=3.3.0",
        "delta-spark>=2.0.0"
    ],
    entry_points={
        "console_scripts": [
            "run_producer = ingest_engine.core.producer.producer_to_landing:main",
            "run_landing_to_bronze = ingest_engine.core.engine.batch_ingest:main",
            "run_bronze_to_silver = ingest_engine.core.engine.to_silver:main",
            "run_silver_to_gold = ingest_engine.core.engine.to_gold:main"
        ]
    }
)
