import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Trusted Customers
TrustedCustomers_node1706277954249 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://eric-project-stedi/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="TrustedCustomers_node1706277954249",
)

# Script generated for node Accelerometer
Accelerometer_node1706277871068 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://eric-project-stedi/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="Accelerometer_node1706277871068",
)

# Script generated for node Join
TrustedCustomers_node1706277954249DF = TrustedCustomers_node1706277954249.toDF()
Accelerometer_node1706277871068DF = Accelerometer_node1706277871068.toDF()
Join_node1706278014611 = DynamicFrame.fromDF(
    TrustedCustomers_node1706277954249DF.join(
        Accelerometer_node1706277871068DF,
        (
            TrustedCustomers_node1706277954249DF["email"]
            == Accelerometer_node1706277871068DF["user"]
        ),
        "left",
    ),
    glueContext,
    "Join_node1706278014611",
)

# Script generated for node Amazon S3
AmazonS3_node1706278350423 = glueContext.write_dynamic_frame.from_options(
    frame=Join_node1706278014611,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://eric-project-stedi/customer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="AmazonS3_node1706278350423",
)

job.commit()
