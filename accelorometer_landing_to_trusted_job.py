import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node customer_trusted
customer_trusted_node1706263595490 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://eric-project-stedi/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="customer_trusted_node1706263595490",
)

# Script generated for node accelerometer_landing
accelerometer_landing_node1706262014228 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://eric-project-stedi/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="accelerometer_landing_node1706262014228",
)

# Script generated for node Join
Join_node1706263705882 = Join.apply(
    frame1=accelerometer_landing_node1706262014228,
    frame2=customer_trusted_node1706263595490,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Join_node1706263705882",
)

# Script generated for node Drop Fields
DropFields_node1706267508285 = DropFields.apply(
    frame=Join_node1706263705882,
    paths=["email", "phone"],
    transformation_ctx="DropFields_node1706267508285",
)

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1706263832067 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1706267508285,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://eric-project-stedi/accelerometer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="accelerometer_trusted_node1706263832067",
)

job.commit()
