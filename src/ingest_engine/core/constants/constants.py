# --- Seguridad y Key Vault ---
SECRET_SCOPE = "ingest-task-scope-01"
KAFKA_SECRET_KEY = "kafka-api-secret"
JAAS_TEMPLATE = 'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{}" password="{}";'

# --- Formatos de Spark ---
FORMAT_KAFKA = "kafka"
FORMAT_DELTA = "delta"
FORMAT_JSON = "json"
FORMAT_CLOUD_FILES = "cloudFiles"

# --- Modos de Escritura ---
MODE_APPEND = "append"
MODE_OVERWRITE = "overwrite"

# --- Columnas del Sistema ---
COL_VALUE = "value"
COL_DATA = "data"
COL_SOURCE_SYSTEM = "source_system"
COL_INGEST_TIMESTAMP = "ingest_timestamp"
COL_SOURCE_FILE = "source_file"

# --- Tipos y Nombres de Datasets ---
TYPE_KAFKA = "kafka"
TYPE_FILE = "file"
DATASET_EVENTS = "events"
DATASET_SALES = "sales"

# --- Sistemas de Origen (para el metadata) ---
SOURCE_KAFKA = "kafka"
SOURCE_FILE = "file"

# --- Unity Catalog / Esquemas de Tablas ---
SCHEMA_GOLD = "adbingesttask01.gold"
TABLE_GOLD_SALES = f"{SCHEMA_GOLD}.sales_daily"