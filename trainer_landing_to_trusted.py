import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://irfan-stedi-lakehouse/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="S3bucket_node1",
)

# Script generated for node Amazon S3
AmazonS3_node1678517245687 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://irfan-stedi-lakehouse/customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1678517245687",
)

# Script generated for node Renamed keys for ApplyMapping
RenamedkeysforApplyMapping_node1678517350700 = ApplyMapping.apply(
    frame=AmazonS3_node1678517245687,
    mappings=[
        ("serialNumber", "string", "`(right) serialNumber`", "string"),
        ("birthDay", "string", "`(right) birthDay`", "string"),
        ("shareWithPublicAsOfDate", "bigint", "shareWithPublicAsOfDate", "long"),
        ("shareWithResearchAsOfDate", "bigint", "shareWithResearchAsOfDate", "long"),
        ("registrationDate", "bigint", "registrationDate", "long"),
        ("customerName", "string", "`(right) customerName`", "string"),
        ("email", "string", "`(right) email`", "string"),
        ("lastUpdateDate", "bigint", "lastUpdateDate", "long"),
        ("phone", "string", "`(right) phone`", "string"),
        ("shareWithFriendsAsOfDate", "bigint", "shareWithFriendsAsOfDate", "long"),
    ],
    transformation_ctx="RenamedkeysforApplyMapping_node1678517350700",
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = Join.apply(
    frame1=S3bucket_node1,
    frame2=RenamedkeysforApplyMapping_node1678517350700,
    keys1=["serialNumber"],
    keys2=["`(right) serialNumber`"],
    transformation_ctx="ApplyMapping_node2",
)

# Script generated for node Drop Fields
DropFields_node1678517744538 = DropFields.apply(
    frame=ApplyMapping_node2,
    paths=[
        "`(right) serialNumber`",
        "`(right) birthDay`",
        "`(right) customerName`",
        "`(right) email`",
        "`(right) phone`",
    ],
    transformation_ctx="DropFields_node1678517744538",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1678517819698 = DynamicFrame.fromDF(
    DropFields_node1678517744538.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1678517819698",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropDuplicates_node1678517819698,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://irfan-stedi-lakehouse/step_trainer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node3",
)

job.commit()
