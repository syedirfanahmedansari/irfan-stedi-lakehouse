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

# Script generated for node Amazon S3
AmazonS3_node1678447805637 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://irfan-stedi-lakehouse/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1678447805637",
)

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://irfan-stedi-lakehouse/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="S3bucket_node1",
)

# Script generated for node Join
Join_node2 = Join.apply(
    frame1=S3bucket_node1,
    frame2=AmazonS3_node1678447805637,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Join_node2",
)

# Script generated for node Drop Fields
DropFields_node1678448045403 = DropFields.apply(
    frame=Join_node2,
    paths=["z", "timeStamp", "user", "y", "x", "email"],
    transformation_ctx="DropFields_node1678448045403",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1678511841743 = DynamicFrame.fromDF(
    DropFields_node1678448045403.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1678511841743",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropDuplicates_node1678511841743,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://irfan-stedi-lakehouse/customer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node3",
)

job.commit()
