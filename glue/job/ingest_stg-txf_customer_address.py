
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
# import time
import sys
import boto3
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from urllib.parse import urlsplit
from botocore.config import Config
from botocore.exceptions import ClientError
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

INPUT_DATE = None  # Otomatis H-1 WIB. Untuk manual, isi tanggal contohnya "2026-03-26".
DATE_PROCESS = None  # None = tanggal proses saat ini di WIB.
BASE_PATH = "s3://<DATA_BUCKET_NAME>/bronze-stg/customer_address_csv/"
OUTPUT_PATH = "s3://<DATA_BUCKET_NAME>/silver-txf/txf_customers_address/"
SOURCE_DATABASE = "bronze_stg"
SOURCE_TABLE = "customer_address_csv"
TARGET_DATABASE = "silver_txf"
TARGET_TABLE = "txf_customers_address"
COLUMNS = ["id", "customer_id", "address", "city", "province", "created_at"]

names = [name for name in ("JOB_NAME", "input_date", "date_process")
         if any(arg == f"--{name}" or arg.startswith(f"--{name}=") for arg in sys.argv)]
args = getResolvedOptions(sys.argv, names) if names else {}
INPUT_DATE = args.get("input_date", INPUT_DATE)
DATE_PROCESS = args.get("date_process", DATE_PROCESS)
job.init(args.get("JOB_NAME", "nb_ingest_stg_customer_address"), args)

spark.conf.set("spark.sql.session.timeZone", "Asia/Jakarta")
spark.conf.set("spark.sql.shuffle.partitions", "4")
spark.conf.set("spark.sql.caseSensitive", "true")
config = Config(retries={"mode": "standard", "total_max_attempts": 1},
                connect_timeout=5, read_timeout=10)
s3 = boto3.client("s3", config=config)
glue = boto3.client("glue", config=config)
def validate_date(value):
    parsed = datetime.strptime(value, "%Y-%m-%d")
    if parsed.strftime("%Y-%m-%d") != value:
        raise ValueError("Tanggal harus YYYY-MM-DD")
    return value


def get_file_path(ds):
    filename = f"customer_address_{validate_date(ds).replace('-', '')}.csv"
    return BASE_PATH.rstrip("/") + "/" + filename


def signature(info):
    return tuple(info.get(key) for key in
                 ("ContentLength", "ETag", "LastModified", "VersionId"))

def assert_unique_customer(frame, label):
    if frame.filter(F.col("customer_id").isNull()
                    | (F.trim(F.col("customer_id")) == "")).limit(1).count():
        raise ValueError(f"{label}: customer_id null/kosong")
    if frame.groupBy("customer_id").count().filter(F.col("count") > 1).limit(1).count():
        raise ValueError(f"{label}: customer_id duplikat")


def find_latest_snapshot():
    uri = urlsplit(OUTPUT_PATH)
    root = uri.path.lstrip("/").rstrip("/") + "/"
    dates = []
    for page in s3.get_paginator("list_objects_v2").paginate(
            Bucket=uri.netloc, Prefix=root, Delimiter="/"):
        if any(obj["Key"].endswith(".parquet") for obj in page.get("Contents", [])):
            raise ValueError("Target masih memiliki Parquet tanpa partisi; migrasikan output lama dahulu")
        for item in page.get("CommonPrefixes", []):
            folder = item["Prefix"][len(root):].rstrip("/")
            if not folder.startswith("date_process="):
                raise ValueError(f"Folder target tidak dikenal: {folder}")
            dates.append(validate_date(folder.split("=", 1)[1]))
    return max(dates) if dates else None
run_date = datetime.now(ZoneInfo("Asia/Jakarta")).date()
today = run_date.isoformat()
yesterday = (run_date - timedelta(days=1)).isoformat()
# Prioritas: --input_date dari job, INPUT_DATE manual di cell, lalu H-1 WIB.
input_date = validate_date(yesterday if INPUT_DATE is None else INPUT_DATE)

date_process = validate_date(DATE_PROCESS or today)
file_path = get_file_path(input_date)
print(f"Source: {file_path}")
print(f"Output: {OUTPUT_PATH}date_process={date_process}/")

uri = urlsplit(file_path)

try:
    file_info = s3.head_object(
        Bucket=uri.netloc,
        Key=uri.path.lstrip("/")
    )
except ClientError as error:
    if (
        error.operation_name == "HeadObject"
        and error.response["Error"]["Code"]
        in ("404", "NoSuchKey", "NotFound")
    ):
        raise FileNotFoundError(
            f"File tidak ditemukan: {file_path}"
        ) from error
    raise

if file_info["ContentLength"] == 0:
    raise ValueError("File CSV kosong")

stable_signature = signature(file_info)
table = glue.get_table(DatabaseName=SOURCE_DATABASE, Name=SOURCE_TABLE)["Table"]
descriptor = table["StorageDescriptor"]
if descriptor["Location"].rstrip("/") != BASE_PATH.rstrip("/"):
    raise ValueError("Location tabel source berbeda dari BASE_PATH; periksa konfigurasi")
columns = descriptor["Columns"]
if [c["Name"] for c in columns] != COLUMNS or any(c["Type"] != "string" for c in columns):
    raise ValueError("Schema source harus enam kolom string sesuai header CSV")

source_schema = StructType([StructField(c["Name"], StringType(), True) for c in columns])
source_df = (spark.read.schema(source_schema)
             .option("header", "true")
             .option("sep", "|@~")
             .option("enforceSchema", "false")
             .option("mode", "FAILFAST")
             .csv(file_path))
dyf = DynamicFrame.fromDF(source_df, glueContext, "src_customer_address")
dyf.printSchema()
df = dyf.toDF()
converted = df.withColumn("_created_at", F.expr("try_cast(created_at AS timestamp)"))
invalid_timestamp = (F.col("created_at").isNotNull()
                     & (F.trim(F.col("created_at")) != "")
                     & F.col("_created_at").isNull())
if converted.filter(invalid_timestamp).limit(1).count():
    raise ValueError("Ada created_at yang tidak dapat dikonversi menjadi timestamp")

incoming = converted.select(
    F.col("id").cast("bigint").alias("id"),
    F.col("customer_id").cast("bigint").alias("customer_id"),
    F.col("address").cast("string").alias("address"),
    F.col("city").cast("string").alias("city"),
    F.col("province").cast("string").alias("province"),
    F.date_trunc(
        "millisecond", F.col("_created_at")
    ).alias("created_at"),
    F.lit(datetime.now(timezone.utc))
     .cast("timestamp")
     .alias("load_timestamp"),
).localCheckpoint(eager=True)

incoming_count = incoming.count()
if incoming_count == 0:
    raise ValueError("CSV tidak memiliki baris data")
assert_unique_customer(incoming, "CSV input")
uri = urlsplit(file_path)
if signature(s3.head_object(Bucket=uri.netloc, Key=uri.path.lstrip("/"))) != stable_signature:
    raise RuntimeError("Object CSV berubah selama pembacaan")

latest_date = find_latest_snapshot()
if latest_date and latest_date > date_process:
    raise ValueError("date_process tidak boleh lebih lama dari snapshot TXF terbaru")
if latest_date:
    previous = spark.read.parquet(f"{OUTPUT_PATH}date_process={latest_date}/").select(
        "id", "customer_id", "address", "city", "province", "created_at", "load_timestamp")
    previous = (
        previous
        .withColumn("id", F.col("id").cast("bigint"))
        .withColumn("customer_id", F.col("customer_id").cast("bigint"))
    )
    if previous.dtypes != incoming.dtypes:
        raise ValueError("Tipe data output lama berbeda; migrasikan terlebih dahulu")
    assert_unique_customer(previous, "TXF sebelumnya")
    merged = previous.alias("old").join(
        incoming.alias("new"), F.col("old.customer_id") == F.col("new.customer_id"), "full_outer")
    output_columns = [
        F.when(F.col("old.customer_id").isNotNull(), F.col("old.id"))
         .otherwise(F.col("new.id")).alias("id"),
        F.coalesce(F.col("old.customer_id"), F.col("new.customer_id")).alias("customer_id"),
    ]
    for name in ["address", "city", "province", "created_at", "load_timestamp"]:
        output_columns.append(
            F.when(F.col("new.customer_id").isNotNull(), F.col(f"new.{name}"))
             .otherwise(F.col(f"old.{name}")).alias(name))
    txf_df = merged.select(*output_columns)
else:
    txf_df = incoming

txf_df = txf_df.withColumn("date_process", F.lit(date_process).cast("date")).localCheckpoint(eager=True)
txf_count = txf_df.count()
DyF = DynamicFrame.fromDF(txf_df, glueContext, "txf_customer_address")
print(f"Input={incoming_count}; snapshot TXF={txf_count}; date_process={date_process}")
DyF.printSchema()
# Preflight Catalog sebelum menghapus output partisi.
try:
    existing = glue.get_table(DatabaseName=TARGET_DATABASE, Name=TARGET_TABLE)["Table"]
except glue.exceptions.EntityNotFoundException:
    existing = None

if existing:
    if existing.get("Parameters", {}).get("table_type", "").upper() == "ICEBERG":
        raise ValueError("Target harus external table Parquet biasa, bukan Iceberg")
    if existing["StorageDescriptor"]["Location"].rstrip("/") != OUTPUT_PATH.rstrip("/"):
        raise ValueError("Location target Catalog berbeda dari OUTPUT_PATH")
    if [(p["Name"], p["Type"]) for p in existing.get("PartitionKeys", [])] != [("date_process", "date")]:
        raise ValueError("Target Catalog harus partition by date_process date")
    if existing.get("Parameters", {}).get("useGlueParquetWriter", "").lower() != "true":
        raise ValueError("Tambahkan table property useGlueParquetWriter=true di Catalog target dahulu")

try:
    glue.get_database(Name=TARGET_DATABASE)
except glue.exceptions.EntityNotFoundException:
    glue.create_database(DatabaseInput={"Name": TARGET_DATABASE})

# Hapus metadata partisi jika date_process yang sama sudah ada di Catalog.
if existing:
    spark.sql(
        f"ALTER TABLE `{TARGET_DATABASE}`.`{TARGET_TABLE}` "
        f"DROP IF EXISTS PARTITION (date_process='{validate_date(date_process)}')"
    )

partition_path = f"{OUTPUT_PATH.rstrip('/')}/date_process={validate_date(date_process)}/"
# Path dibentuk dari prefix silver tetap + tanggal tervalidasi; tidak pernah memakai BASE_PATH.
glueContext.purge_s3_path(partition_path, options={"retentionPeriod": 0})

s3output = glueContext.getSink(
    path=OUTPUT_PATH,
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=["date_process"],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="s3output",
)
s3output.setCatalogInfo(
    catalogDatabase=TARGET_DATABASE, catalogTableName=TARGET_TABLE
)
s3output.setFormat("glueparquet")
s3output.writeFrame(DyF)
job.commit()
print(f"SUCCESS: {incoming_count} input rows -> {txf_count} snapshot rows")
print(f"Catalog: {TARGET_DATABASE}.{TARGET_TABLE}; partition: {date_process}")
job.commit()