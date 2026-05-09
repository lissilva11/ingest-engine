# Ingest Engine

Motor de ingesta para lakehouse en Databricks. Implementa pipelines batch (Autoloader) y streaming (Kafka) que escriben Delta en ADLS Gen2 en capas landing -> bronze -> silver -> gold.

## Requisitos
- Databricks Workspace con cluster que soporte Spark 3.x y Delta
- Cuenta ADLS Gen2 accesible desde Databricks
- Secrets en Databricks con credenciales (Service Principal o storage key)
- Python 3.8+ para empaquetado local

## Instalación local y empaquetado
1. Crear virtualenv e instalar dependencias:
```bash
pip install -r requirements.txt
```
2. Empaquetar como wheel:
```bash
python setup.py bdist_wheel
```

3. Subir el wheel a DBFS o a un repositorio accesible por Databricks.

## Despliegue en Databricks
- Crear Secret Scope y añadir secretos necesarios (`storage-account-key`).
- Subir `configs/dataset_config.yaml` y `configs/schemas/*` a DBFS (por ejemplo `dbfs:/configs/...`).
- Crear Job tipo Spark Python task:
- Package: path al wheel en DBFS
- Entry point: `ingest_engine.main:cli_entry`
- Parameters: `--config dbfs:/configs/dataset_config.yaml`
- Cluster: job cluster o existing cluster

## Ejecución local con spark-submit (si tienes Spark local)
```bash
spark-submit --py-files dist/ingest_engine-0.1.0-py3-none-any.whl src/ingest_engine/main.py --config configs/dataset_config.yaml
```

## Notas operativas
- Cada dataset debe tener `checkpointLocation` único.
- Para batch con Autoloader se usa `trigger(availableNow=True)` para procesar nuevos archivos y terminar.
- Para streaming use `query.awaitTermination()` y gestione el Job en Databricks como tarea persistente.
- Después de ingestas, ejecutar `OPTIMIZE` y `VACUUM` según políticas.

## Tests
Ejecutar:
```bash
pip install -e .
pytest -q
```
