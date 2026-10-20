import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame
import gs_derived

# Script generated for node duplicate_handling
def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    import sys
    from datetime import datetime
    from awsglue.utils import getResolvedOptions
    from awsglue.dynamicframe import DynamicFrame, DynamicFrameCollection
    from pyspark.sql import functions as F

    spark = glueContext.spark_session

    TARGET_DATABASE = "silver_txf"
    TARGET_TABLE = "txf_after_sales"
    OUTPUT_PATH = (
        "s3://s3-datalake-dev/silver-txf/txf_after_sales/"
    )
    TEMP_ROOT = (
        "s3://s3-datalake-dev/temporary/ingest_txf_after_sales/"
    )

    # Wajib job run sungguhan; jangan jalankan node ini melalui Data preview.
    args = getResolvedOptions(sys.argv, ["JOB_RUN_ID"])
    run_id = args["JOB_RUN_ID"]

    if not run_id or any(c not in
                         "abcdefghijklmnopqrstuvwxyz"
                         "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                         "0123456789_-" for c in run_id):
        raise ValueError("JOB_RUN_ID tidak valid")

    temp_path = f"{TEMP_ROOT}{run_id}/"

    columns = [
        "service_ticket", "vin", "customer_id", "model",
        "service_date", "service_type", "created_at",
        "load_timestamp", "tran_date"
    ]

    def validate_date(value):
        if not isinstance(value, str):
            raise ValueError(f"tran_date tidak valid: {value!r}")

        parsed = datetime.strptime(value, "%Y-%m-%d")
        if parsed.strftime("%Y-%m-%d") != value:
            raise ValueError(f"Format tran_date tidak valid: {value!r}")

        return value

    def normalize(df):
        return df.select(
            F.col("service_ticket").cast("string").alias("service_ticket"),
            F.col("vin").cast("string").alias("vin"),
            F.col("customer_id").cast("string").alias("customer_id"),
            F.col("model").cast("string").alias("model"),
            F.col("service_date").cast("date").alias("service_date"),
            F.col("service_type").cast("string").alias("service_type"),
            F.date_trunc(
                "MILLISECOND", F.col("created_at").cast("timestamp")
            ).alias("created_at"),
            F.col("load_timestamp").cast("timestamp").alias("load_timestamp"),
            F.col("tran_date").cast("date").alias("tran_date")
        )

    def validate_rows(df, label):
        invalid = (
            F.col("service_ticket").isNull()
            | (F.trim(F.col("service_ticket")) == "")
            | F.col("service_date").isNull()
            | F.col("created_at").isNull()
            | F.col("tran_date").isNull()
            | (
                F.col("tran_date")
                != F.date_format(F.col("service_date"), "yyyy-MM-dd")
            )
        )

        if df.filter(invalid).limit(1).count():
            raise ValueError(f"{label}: tiket atau tanggal tidak valid")

        duplicates = (
            df.groupBy("service_ticket")
              .count()
              .filter(F.col("count") > 1)
        )
        if duplicates.limit(1).count():
            raise ValueError(
                f"{label}: ada beberapa baris untuk service_ticket yang sama"
            )

    def dates_of(df):
        return {
            validate_date(row["tran_date"])
            for row in df.select("tran_date").distinct().collect()
        }

    # Memeriksa file langsung, termasuk bila metadata Catalog belum tersedia.
    def has_parquet(path):
        jpath = spark._jvm.org.apache.hadoop.fs.Path(path)
        fs = jpath.getFileSystem(spark._jsc.hadoopConfiguration())

        if not fs.exists(jpath):
            return False

        files = fs.listFiles(jpath, True)
        while files.hasNext():
            name = files.next().getPath().getName().lower()
            if name.endswith(".parquet"):
                return True
        return False

    source = dfc.select(list(dfc.keys())[0])
    incoming = normalize(source.toDF()).localCheckpoint(eager=True)

    # Fail sebelum menghapus apa pun jika input kosong.
    if not incoming.limit(1).count():
        raise ValueError("Tidak ada data untuk diproses; target tidak diubah")

    validate_rows(incoming, "Input")
    incoming_dates = dates_of(incoming)

    if has_parquet(OUTPUT_PATH):
        old_raw = spark.read.parquet(OUTPUT_PATH)

        if "tran_date" not in old_raw.columns:
            raise ValueError(
                "Target lama belum berpartisi tran_date; migrasikan dahulu"
            )

        previous = normalize(old_raw)
        validate_rows(previous, "Silver lama")

        incoming_keys = incoming.select("service_ticket")

        # Sertakan tanggal lama bila tiket berpindah service_date.
        old_matching = previous.join(
            incoming_keys, "service_ticket", "inner"
        )
        affected_dates = incoming_dates | dates_of(old_matching)

        old_affected = previous.filter(
            F.col("tran_date").isin(sorted(affected_dates))
        )

        # Pertahankan tiket yang tidak diganti oleh batch masuk.
        retained = old_affected.join(
            incoming_keys, "service_ticket", "left_anti"
        )

        replacement = retained.select(*columns).unionByName(
            incoming.select(*columns)
        )
    else:
        affected_dates = incoming_dates
        replacement = incoming.select(*columns)

    # Simpan lengkap sebelum menghapus data lama.
    replacement.write.mode("errorifexists").parquet(temp_path)

    staged = spark.read.parquet(temp_path).select(*columns)
    validate_rows(staged, "Hasil gabungan")
    print(f"Data pengganti tersimpan di: {temp_path}")

    existing = spark.catalog.tableExists(
        f"{TARGET_DATABASE}.{TARGET_TABLE}"
    )

    for tran_date in sorted(affected_dates):
        if existing:
            spark.sql(
                f"ALTER TABLE `{TARGET_DATABASE}`.`{TARGET_TABLE}` "
                f"DROP IF EXISTS PARTITION (tran_date='{tran_date}')"
            )

        partition_path = (
            f"{OUTPUT_PATH.rstrip('/')}/tran_date={tran_date}/"
        )

        glueContext.purge_s3_path(
            partition_path,
            options={"retentionPeriod": 0},
        )

        if has_parquet(partition_path):
            raise RuntimeError(
                f"File lama belum bersih: {partition_path}"
            )

        print(f"Partisi dibersihkan: {tran_date}")

    result = DynamicFrame.fromDF(
        staged, glueContext, "replacement_after_sales"
    )

    return DynamicFrameCollection({"result": result}, glueContext)    
# Script generated for node filter_initial_or_daily
def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    import sys
    from datetime import datetime, timedelta
    from zoneinfo import ZoneInfo
    from awsglue.utils import getResolvedOptions
    from awsglue.dynamicframe import DynamicFrame, DynamicFrameCollection
    from pyspark.sql import functions as F
    names = [
        name
        for name in ["is_initial_load", "input_date"]
        if any(
            arg == f"--{name}" or arg.startswith(f"--{name}=")
            for arg in sys.argv
        )
    ]
    args = getResolvedOptions(sys.argv, names) if names else {}

    initial = args.get("is_initial_load", "false").strip().lower()
    if initial not in ("true", "false"):
        raise ValueError("--is_initial_load harus true atau false")

    source = dfc.select(list(dfc.keys())[0])

    if initial == "true":
        print("Initial load: seluruh data Bronze")
        return DynamicFrameCollection({"result": source}, glueContext)

    input_date = args.get("input_date", "").strip()
    if not input_date:
        input_date = (
            datetime.now(ZoneInfo("Asia/Jakarta")).date()
            - timedelta(days=1)
        ).isoformat()

    process_date = datetime.strptime(input_date, "%Y-%m-%d").date()

    df = source.toDF().filter(
        F.to_date(F.col("created_at")) == F.lit(process_date)
    )

    print(f"Load harian: created_at tanggal {process_date}")

    result = DynamicFrame.fromDF(df, glueContext, "filtered")
    return DynamicFrameCollection({"result": result}, glueContext)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node src_postgresql
src_postgresql_node1792156802519 = glueContext.create_dynamic_frame.from_options(
    connection_type = "postgresql",
    connection_options = {
        "useConnectionProperties": "true",
        "dbtable": "public.stg_after_sales_raw",
        "connectionName": "Postgresql connection",
    },
    transformation_ctx = "src_postgresql_node1792156802519"
)

# Script generated for node filter_initial_or_daily
filter_initial_or_daily_node1792152814596 = MyTransform(glueContext, DynamicFrameCollection({"src_postgresql_node1792156802519": src_postgresql_node1792156802519}, glueContext))

# Script generated for node select_filtered_data
select_filtered_data_node1792153775269 = SelectFromCollection.apply(dfc=filter_initial_or_daily_node1792152814596, key=list(filter_initial_or_daily_node1792152814596.keys())[0], transformation_ctx="select_filtered_data_node1792153775269")

# Script generated for node map_after_sales
map_after_sales_node1792153886385 = ApplyMapping.apply(frame=select_filtered_data_node1792153775269, mappings=[("service_type", "string", "service_type", "string"), ("service_date", "string", "service_date", "date"), ("service_ticket", "string", "service_ticket", "string"), ("created_at", "string", "created_at", "timestamp"), ("vin", "string", "vin", "string"), ("model", "string", "model", "string"), ("customer_id", "string", "customer_id", "bigint")], transformation_ctx="map_after_sales_node1792153886385")

# Script generated for node add_load_timestamp
add_load_timestamp_node1792154058583 = map_after_sales_node1792153886385.gs_derived(colName="load_timestamp", expr="current_timestamp()")

# Script generated for node add_tran_date
add_tran_date_node1792154168281 = add_load_timestamp_node1792154058583.gs_derived(colName="tran_date", expr="date_format(service_date, 'yyyy-MM-dd')")

# Script generated for node duplicate_handling
duplicate_handling_node1792154217797 = MyTransform(glueContext, DynamicFrameCollection({"add_tran_date_node1792154168281": add_tran_date_node1792154168281}, glueContext))

# Script generated for node select_replacement_data
select_replacement_data_node1792155766114 = SelectFromCollection.apply(dfc=duplicate_handling_node1792154217797, key=list(duplicate_handling_node1792154217797.keys())[0], transformation_ctx="select_replacement_data_node1792155766114")

# Script generated for node out_after_sales
EvaluateDataQuality().process_rows(frame=select_replacement_data_node1792155766114, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1792154439091", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
out_after_sales_node1792155789820 = glueContext.getSink(path="s3://s3-datalake-dev/silver-txf/txf_after_sales/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=["tran_date"], enableUpdateCatalog=True, transformation_ctx="out_after_sales_node1792155789820")
out_after_sales_node1792155789820.setCatalogInfo(catalogDatabase="silver_txf",catalogTableName="txf_after_sales")
out_after_sales_node1792155789820.setFormat("glueparquet", compression="snappy")
out_after_sales_node1792155789820.writeFrame(select_replacement_data_node1792155766114)
job.commit()