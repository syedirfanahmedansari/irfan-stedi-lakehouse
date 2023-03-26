import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs


def sparkAggregate(
    glueContext, parentFrame, groups, aggs, transformation_ctx
) -> DynamicFrame:
    aggsFuncs = []
    for column, func in aggs:
        aggsFuncs.append(getattr(SqlFuncs, func)(column))
    result = (
        parentFrame.toDF().groupBy(*groups).agg(*aggsFuncs)
        if len(groups) > 0
        else parentFrame.toDF().agg(*aggsFuncs)
    )
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


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
        "paths": ["s3://irfan-stedi-lakehouse/step_trainer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="S3bucket_node1",
)

# Script generated for node Amazon S3
AmazonS3_node1678666019053 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://irfan-stedi-lakehouse/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1678666019053",
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = Join.apply(
    frame1=S3bucket_node1,
    frame2=AmazonS3_node1678666019053,
    keys1=["sensorReadingTime"],
    keys2=["timeStamp"],
    transformation_ctx="ApplyMapping_node2",
)

# Script generated for node Drop Fields
DropFields_node1678666285606 = DropFields.apply(
    frame=ApplyMapping_node2,
    paths=[
        "`(right) customerName`",
        "`(right) email`",
        "`(right) serialNumber`",
        "`(right) birthDay`",
        "`(right) phone`",
        "timeStamp",
        "user",
    ],
    transformation_ctx="DropFields_node1678666285606",
)

# Script generated for node Aggregate
Aggregate_node1678666368926 = sparkAggregate(
    glueContext,
    parentFrame=DropFields_node1678666285606,
    groups=[],
    aggs=[["distanceFromObject", "sum"], ["z", "sum"], ["y", "sum"], ["x", "sum"]],
    transformation_ctx="Aggregate_node1678666368926",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=Aggregate_node1678666368926,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://irfan-stedi-lakehouse/step_trainer/machine_learning_curated/",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node3",
)

job.commit()
