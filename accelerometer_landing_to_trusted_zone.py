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
        "paths": ["s3://irfan-stedi-lakehouse/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="S3bucket_node1",
)

# Script generated for node Amazon S3
AmazonS3_node1678444936761 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://irfan-stedi-lakehouse/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1678444936761",
)

# Script generated for node Customer_accelerator_join
Customer_accelerator_join_node2 = Join.apply(
    frame1=S3bucket_node1,
    frame2=AmazonS3_node1678444936761,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Customer_accelerator_join_node2",
)

# Script generated for node Drop Fields
DropFields_node1678445338481 = DropFields.apply(
    frame=Customer_accelerator_join_node2,
    paths=[
        "serialNumber",
        "shareWithPublicAsOfDate",
        "birthDay",
        "registrationDate",
        "shareWithResearchAsOfDate",
        "customerName",
        "email",
        "lastUpdateDate",
        "phone",
        "shareWithFriendsAsOfDate",
    ],
    transformation_ctx="DropFields_node1678445338481",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1678507036726 = DynamicFrame.fromDF(
    DropFields_node1678445338481.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1678507036726",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropDuplicates_node1678507036726,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://irfan-stedi-lakehouse/accelerometer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node3",
)

job.commit()
