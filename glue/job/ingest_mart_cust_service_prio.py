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
from pyspark.sql import functions as SqlFuncs

# Script generated for node clear_previous_mart
def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    import sys
    from awsglue.utils import getResolvedOptions
    from awsglue.dynamicframe import DynamicFrameCollection

    if not getResolvedOptions(sys.argv, []).get("JOB_RUN_ID"):
        raise ValueError("Gunakan Run job, bukan Data preview.")

    frame = dfc.select(list(dfc.keys())[0])

    if frame.count() == 0:
        raise ValueError("Hasil mart kosong; output lama tidak dihapus.")

    glueContext.purge_s3_path(
        "s3://s3-datalake-dev/gold-mart/mart_cust_service_prio/",
        options={"retentionPeriod": 0}
    )

    return DynamicFrameCollection({"result": frame}, glueContext)    
def sparkAggregate(glueContext, parentFrame, groups, aggs, transformation_ctx) -> DynamicFrame:
    aggsFuncs = []
    for column, func in aggs:
        aggsFuncs.append(getattr(SqlFuncs, func)(column))
    result = parentFrame.toDF().groupBy(*groups).agg(*aggsFuncs) if len(groups) > 0 else parentFrame.toDF().agg(*aggsFuncs)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)

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

# Script generated for node src_after_sales
src_after_sales_node1792247727827 = glueContext.create_dynamic_frame.from_catalog(database="silver_txf", table_name="txf_after_sales", transformation_ctx="src_after_sales_node1792247727827")

# Script generated for node src_customers
src_customers_node1792250608849 = glueContext.create_dynamic_frame.from_catalog(database="silver_txf", table_name="txf_customers", transformation_ctx="src_customers_node1792250608849")

# Script generated for node src_address
src_address_node1792252842567 = glueContext.create_dynamic_frame.from_catalog(database="silver_txf", table_name="txf_customers_address", transformation_ctx="src_address_node1792252842567")

# Script generated for node Change Schema
ChangeSchema_node1792247916975 = ApplyMapping.apply(frame=src_after_sales_node1792247727827, mappings=[("service_date", "date", "service_date", "date"), ("service_ticket", "string", "service_ticket", "string"), ("vin", "string", "vin", "string"), ("customer_id", "long", "customer_id", "bigint")], transformation_ctx="ChangeSchema_node1792247916975")

# Script generated for node latest_customer_date
latest_customer_date_node1792250864459 = sparkAggregate(glueContext, parentFrame = src_customers_node1792250608849, groups = [], aggs = [["date_process", "max"]], transformation_ctx = "latest_customer_date_node1792250864459")

# Script generated for node latest_address_date
latest_address_date_node1792254126909 = sparkAggregate(glueContext, parentFrame = src_address_node1792252842567, groups = [], aggs = [["date_process", "max"]], transformation_ctx = "latest_address_date_node1792254126909")

# Script generated for node add_periode
add_periode_node1792248031874 = ChangeSchema_node1792247916975.gs_derived(colName="periode", expr="year(service_date)")

# Script generated for node rename_latest_customer_date
rename_latest_customer_date_node1792251451072 = RenameField.apply(frame=latest_customer_date_node1792250864459, old_name="`max(date_process)`", new_name="latest_customer_date", transformation_ctx="rename_latest_customer_date_node1792251451072")

# Script generated for node rename_latest_address_date
rename_latest_address_date_node1792254208100 = RenameField.apply(frame=latest_address_date_node1792254126909, old_name="`max(date_process)`", new_name="latest_address_date", transformation_ctx="rename_latest_address_date_node1792254208100")

# Script generated for node count_services
count_services_node1792248083039 = sparkAggregate(glueContext, parentFrame = add_periode_node1792248031874, groups = ["periode", "vin", "customer_id"], aggs = [["service_ticket", "count"]], transformation_ctx = "count_services_node1792248083039")

# Script generated for node select_latest_customers
select_latest_customers_node1792251538923 = Join.apply(frame1=src_customers_node1792250608849, frame2=rename_latest_customer_date_node1792251451072, keys1=["date_process"], keys2=["latest_customer_date"], transformation_ctx="select_latest_customers_node1792251538923")

# Script generated for node select_latest_address
select_latest_address_node1792257909401 = Join.apply(frame1=src_address_node1792252842567, frame2=rename_latest_address_date_node1792254208100, keys1=["date_process"], keys2=["latest_address_date"], transformation_ctx="select_latest_address_node1792257909401")

# Script generated for node rename_count_service
rename_count_service_node1792250059344 = RenameField.apply(frame=count_services_node1792248083039, old_name="`count(service_ticket)`", new_name="count_service", transformation_ctx="rename_count_service_node1792250059344")

# Script generated for node customer_lookup
customer_lookup_node1792252489502 = ApplyMapping.apply(frame=select_latest_customers_node1792251538923, mappings=[("id", "long", "lookup_customer_id", "bigint"), ("name", "string", "customer_name", "string")], transformation_ctx="customer_lookup_node1792252489502")

# Script generated for node address_lookup
address_lookup_node1792258044316 = ApplyMapping.apply(frame=select_latest_address_node1792257909401, mappings=[("customer_id", "long", "customer_id", "bigint"), ("address", "string", "address", "string")], transformation_ctx="address_lookup_node1792258044316")

# Script generated for node join_customer
join_customer_node1792252712012 = Join.apply(frame1=rename_count_service_node1792250059344, frame2=customer_lookup_node1792252489502, keys1=["customer_id"], keys2=["lookup_customer_id"], transformation_ctx="join_customer_node1792252712012")

# Script generated for node Renamed keys for join_address
Renamedkeysforjoin_address_node1792258485488 = ApplyMapping.apply(frame=address_lookup_node1792258044316, mappings=[("customer_id", "bigint", "right_customer_id", "bigint"), ("address", "string", "right_address", "string")], transformation_ctx="Renamedkeysforjoin_address_node1792258485488")

# Script generated for node join_address
join_customer_node1792252712012DF = join_customer_node1792252712012.toDF()
address_lookup_node1792258044316DF = address_lookup_node1792258044316.toDF()
join_address_node1792258121260 = DynamicFrame.fromDF(join_customer_node1792252712012DF.join(address_lookup_node1792258044316DF, (join_customer_node1792252712012DF['customer_id'] == address_lookup_node1792258044316DF['customer_id']), "left"), glueContext, "join_address_node1792258121260")

# Script generated for node add_priority
add_priority_node1792258245496 = join_address_node1792258121260.gs_derived(colName="priority", expr="CASE   WHEN count_service > 10 THEN 'HIGH'   WHEN count_service BETWEEN 5 AND 10 THEN 'MED'   ELSE 'LOW' END")

# Script generated for node final_schema
final_schema_node1792258338931 = ApplyMapping.apply(frame=add_priority_node1792258245496, mappings=[("priority", "string", "priority", "string"), ("periode", "int", "periode", "int"), ("vin", "string", "vin", "string"), ("count_service", "long", "count_service", "int"), ("customer_name", "string", "customer_name", "string"), ("address", "string", "address", "string")], transformation_ctx="final_schema_node1792258338931")

# Script generated for node add_load_timestamp
add_load_timestamp_node1792258692089 = final_schema_node1792258338931.gs_derived(colName="load_timestamp", expr="current_timestamp()")

# Script generated for node clear_previous_mart
clear_previous_mart_node1792258754388 = MyTransform(glueContext, DynamicFrameCollection({"add_load_timestamp_node1792258692089": add_load_timestamp_node1792258692089}, glueContext))

# Script generated for node select_mart
select_mart_node1792259004589 = SelectFromCollection.apply(dfc=clear_previous_mart_node1792258754388, key=list(clear_previous_mart_node1792258754388.keys())[0], transformation_ctx="select_mart_node1792259004589")

# Script generated for node out_customer_service_prio
EvaluateDataQuality().process_rows(frame=select_mart_node1792259004589, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1792252010270", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
out_customer_service_prio_node1792259072766 = glueContext.getSink(path="s3://s3-datalake-dev/gold-mart/mart_cust_service_prio/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="out_customer_service_prio_node1792259072766")
out_customer_service_prio_node1792259072766.setCatalogInfo(catalogDatabase="gold_mart",catalogTableName="mart_cust_service_prio")
out_customer_service_prio_node1792259072766.setFormat("glueparquet", compression="snappy")
out_customer_service_prio_node1792259072766.writeFrame(select_mart_node1792259004589)
job.commit()