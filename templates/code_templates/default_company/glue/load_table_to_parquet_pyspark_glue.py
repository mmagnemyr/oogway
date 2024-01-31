import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node [:REPLACE_TABLE_NAME]
[:REPLACE_TABLE_NAME]_node1687870445927 = glueContext.create_dynamic_frame.from_catalog(
    database="[:REPLACE_DATABASE]",
    table_name="[:REPLACE_TABLE_NAME]",
    transformation_ctx="[:REPLACE_TABLE_NAME]_node1687870445927",
)

# Script generated for node SQL Query
SqlQuery0 = """
[:REPLACE_POPULATE_DATA]
"""
SQLQuery_node1687870509669 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={"[:REPLACE_TABLE_NAME]": [:REPLACE_TABLE_NAME]_node1687870445927},
    transformation_ctx="SQLQuery_node1687870509669",
)

# Script generated for node Amazon S3
AmazonS3_node1687871292900 = glueContext.write_dynamic_frame.from_options(
    frame=SQLQuery_node1687870509669,
    connection_type="s3",
    format="glueparquet",
    connection_options={"path": "s3://[:REPLACE_BUCKET]/[:REPLACE_PREFIX]", "partitionKeys": []},
    format_options={"compression": "snappy"},
    transformation_ctx="AmazonS3_node1687871292900",
)

job.commit()