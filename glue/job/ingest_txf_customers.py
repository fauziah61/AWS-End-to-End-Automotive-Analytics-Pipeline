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
    from zoneinfo import ZoneInfo
    from datetime import datetime
    from awsglue.utils import getResolvedOptions
    from awsglue.dynamicframe import DynamicFrame, DynamicFrameCollection
    from pyspark.sql import functions as F

    spark = glueContext.spark_session

    TARGET_DATABASE = "silver_txf"
    TARGET_TABLE = "txf_customers"
    OUTPUT_PATH = (
        "s3://s3-datalake-dev/silver-txf/txf_customers/"
    )
    TEMP_ROOT = (
        "s3://s3-datalake-dev/temporary/ingest_txf_customers/"
    )

    # Wajib job run sungguhan; jangan jalankan node ini melalui Data preview.
    # Glue mendaftarkan JOB_RUN_ID sebagai argumen internal.
    args = getResolvedOptions(sys.argv, [])
    run_id = args.get("JOB_RUN_ID")

    if not run_id or any(c not in
                         "abcdefghijklmnopqrstuvwxyz"
                         "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                         "0123456789_-" for c in run_id):
        raise ValueError("JOB_RUN_ID tidak valid")

    temp_path = f"{TEMP_ROOT}{run_id}/"

    data_columns = [
        "id", "name", "dob", "created_at", "load_timestamp"
    ]
    output_columns = data_columns + ["date_process"]

    def validate_date(value):
        if not isinstance(value, str):
            raise ValueError(f"date_process tidak valid: {value!r}")

        parsed = datetime.strptime(value, "%Y-%m-%d")

        if parsed.strftime("%Y-%m-%d") != value:
            raise ValueError(f"Format date_process tidak valid: {value!r}")

        return value

    def normalize(df):
        return df.select(
            F.col("id").cast("bigint").alias("id"),
            F.col("name").cast("string").alias("name"),
            F.col("dob").cast("date").alias("dob"),
            F.date_trunc(
                "MILLISECOND",
                F.col("created_at").cast("timestamp")
            ).alias("created_at"),
            F.col("load_timestamp").cast("timestamp").alias("load_timestamp"),
            F.col("date_process").cast("date").alias("date_process")
        )

    def validate_rows(df, label):
        invalid = (
            F.col("id").isNull()
            | (F.trim(F.col("id")) == "")
        )

        if df.filter(invalid).limit(1).count():
            raise ValueError(f"{label}: id kosong atau null")

        duplicates = (
            df.groupBy("id")
              .count()
              .filter(F.col("count") > 1)
        )

        if duplicates.limit(1).count():
            raise ValueError(f"{label}: ada id yang duplikat")

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

    # date_process manual; default tanggal proses hari ini di Jakarta.
    has_date_process = any(
        arg == "--date_process" or arg.startswith("--date_process=")
        for arg in sys.argv
    )

    if has_date_process:
        date_process = validate_date(
            getResolvedOptions(sys.argv, ["date_process"])["date_process"]
        )
    else:
        date_process = datetime.now(
            ZoneInfo("Asia/Jakarta")
        ).date().isoformat()

    source = dfc.select(list(dfc.keys())[0])
    incoming = normalize(source.toDF()).localCheckpoint(eager=True)

    if not incoming.limit(1).count():
        raise ValueError("Source customer kosong; target tidak diubah")

    validate_rows(incoming, "Input customer")

    if has_parquet(OUTPUT_PATH):
        old_raw = spark.read.parquet(OUTPUT_PATH)

        if "date_process" not in old_raw.columns:
            raise ValueError(
                "Target lama belum berpartisi date_process"
            )

        available_dates = [
            validate_date(str(row["date_process"]))
            for row in old_raw.select("date_process").distinct().collect()
        ]

        latest_date = max(available_dates)

        if date_process < latest_date:
            raise ValueError(
                "date_process tidak boleh lebih lama dari snapshot terbaru"
            )

        previous = normalize(
            old_raw.filter(
                F.col("date_process").cast("date") == F.lit(latest_date)
            )
        )

        validate_rows(previous, "Snapshot customer sebelumnya")

        # ID yang masuk menggantikan seluruh atribut customer lama.
        retained = previous.join(
            incoming.select("id"),
            "id",
            "left_anti"
        )

        replacement = retained.select(*data_columns).unionByName(
            incoming.select(*data_columns)
        )
    else:
        replacement = incoming.select(*data_columns)

    replacement = replacement.withColumn(
        "date_process",
        F.lit(date_process)
    )

    # Materialisasikan snapshot sebelum drop/purge.
    replacement.write.mode("errorifexists").parquet(temp_path)

    staged = spark.read.parquet(temp_path).select(*output_columns)
    validate_rows(staged, "Snapshot pengganti")

    print(f"Snapshot pengganti: {temp_path}")

    existing = spark.catalog.tableExists(
        f"{TARGET_DATABASE}.{TARGET_TABLE}"
    )

    if existing:
        spark.sql(
            f"ALTER TABLE `{TARGET_DATABASE}`.`{TARGET_TABLE}` "
            f"DROP IF EXISTS PARTITION "
            f"(date_process='{date_process}')"
        )

    partition_path = (
        f"{OUTPUT_PATH.rstrip('/')}/date_process={date_process}/"
    )

    glueContext.purge_s3_path(
        partition_path,
        options={"retentionPeriod": 0}
    )

    if has_parquet(partition_path):
        raise RuntimeError(
            f"File lama belum bersih: {partition_path}"
        )

    result = DynamicFrame.fromDF(
        staged,
        glueContext,
        "replacement_customers"
    )

    return DynamicFrameCollection(
        {"result": result},
        glueContext
    )
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
        "dbtable": "public.stg_customers_raw",
        "connectionName": "Postgresql connection",
    },
    transformation_ctx = "src_postgresql_node1792156802519"
)

# Script generated for node convert_dob
convert_dob_node1792209462771 = src_postgresql_node1792156802519.gs_derived(colName="dob", expr="CASE   WHEN dob RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'     THEN CAST(try_to_timestamp(dob, 'yyyy-MM-dd') AS DATE)   WHEN dob RLIKE '^[0-9]{4}/[0-9]{2}/[0-9]{2}$'     THEN CAST(try_to_timestamp(dob, 'yyyy/MM/dd') AS DATE)   WHEN dob RLIKE '^[0-9]{2}/[0-9]{2}/[0-9]{4}$'     THEN CAST(try_to_timestamp(dob, 'dd/MM/yyyy') AS DATE)   ELSE CAST(NULL AS DATE) END")

# Script generated for node add_load_timestamp
add_load_timestamp_node1792154058583 = convert_dob_node1792209462771.gs_derived(colName="load_timestamp", expr="current_timestamp()")

# Script generated for node duplicate_handling
duplicate_handling_node1792219203222 = MyTransform(glueContext, DynamicFrameCollection({"add_load_timestamp_node1792154058583": add_load_timestamp_node1792154058583}, glueContext))

# Script generated for node Select From Collection
SelectFromCollection_node1792219603930 = SelectFromCollection.apply(dfc=duplicate_handling_node1792219203222, key=list(duplicate_handling_node1792219203222.keys())[0], transformation_ctx="SelectFromCollection_node1792219603930")

# Script generated for node out_customers
EvaluateDataQuality().process_rows(frame=SelectFromCollection_node1792219603930, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1792208964733", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
out_customers_node1792219659862 = glueContext.getSink(path="s3://s3-datalake-dev/silver-txf/txf_customers/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=["date_process"], enableUpdateCatalog=True, transformation_ctx="out_customers_node1792219659862")
out_customers_node1792219659862.setCatalogInfo(catalogDatabase="silver_txf",catalogTableName="txf_customers")
out_customers_node1792219659862.setFormat("glueparquet", compression="snappy")
out_customers_node1792219659862.writeFrame(SelectFromCollection_node1792219603930)
job.commit()