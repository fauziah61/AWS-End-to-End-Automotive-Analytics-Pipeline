import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
import gs_derived

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

# Script generated for node src_customers
src_customers_node1791542747811 = glueContext.create_dynamic_frame.from_catalog(database="bronze_stg", table_name="customer_address_csv", transformation_ctx="src_customers_node1791542747811")

# Script generated for node Change Schema
ChangeSchema_node1791543768813 = ApplyMapping.apply(frame=src_customers_node1791542747811, mappings=[("id", "string", "id", "bigint"), ("customer_id", "string", "customer_id", "bigint"), ("address", "string", "address", "varchar"), ("city", "string", "city", "varchar"), ("province", "string", "province", "varchar"), ("created_at", "string", "created_at", "timestamp")], transformation_ctx="ChangeSchema_node1791543768813")

# Script generated for node add_load_timestamp
add_load_timestamp_node1791543191646 = ChangeSchema_node1791543768813.gs_derived(colName="load_timestamp", expr="current_timestamp()")

# Script generated for node out_customers
EvaluateDataQuality().process_rows(frame=add_load_timestamp_node1791543191646, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1791536058025", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
out_customers_node1791543505414 = glueContext.write_dynamic_frame.from_options(frame=add_load_timestamp_node1791543191646, connection_type="s3", format="glueparquet", connection_options={"path": "s3://<DATA_BUCKET_NAME>/silver-txf/txf_customers_address/", "partitionKeys": []}, format_options={"compression": "uncompressed"}, transformation_ctx="out_customers_node1791543505414")

job.commit()