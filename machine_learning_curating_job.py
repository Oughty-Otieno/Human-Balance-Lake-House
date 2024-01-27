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

# Script generated for node Step landing
Steplanding_node1706277115574 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://eric-project-stedi/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="Steplanding_node1706277115574",
)

# Script generated for node accelerometer
accelerometer_node1706277204144 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://eric-project-stedi/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="accelerometer_node1706277204144",
)

# Script generated for node customer trusted
customertrusted_node1706277145132 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://eric-project-stedi/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="customertrusted_node1706277145132",
)

# Script generated for node Join
Steplanding_node1706277115574DF = Steplanding_node1706277115574.toDF()
accelerometer_node1706277204144DF = accelerometer_node1706277204144.toDF()
Join_node1706277329493 = DynamicFrame.fromDF(
    Steplanding_node1706277115574DF.join(
        accelerometer_node1706277204144DF,
        (
            Steplanding_node1706277115574DF["sensorreadingtime"]
            == accelerometer_node1706277204144DF["timestamp"]
        ),
        "left",
    ),
    glueContext,
    "Join_node1706277329493",
)

# Script generated for node Customer Trusted Join
CustomerTrustedJoin_node1706379138822 = Join.apply(
    frame1=Join_node1706277329493,
    frame2=customertrusted_node1706277145132,
    keys1=["serialNumber"],
    keys2=["serialNumber"],
    transformation_ctx="CustomerTrustedJoin_node1706379138822",
)

# Script generated for node Amazon S3
AmazonS3_node1706277484296 = glueContext.write_dynamic_frame.from_options(
    frame=CustomerTrustedJoin_node1706379138822,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://eric-project-stedi/machine learning/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="AmazonS3_node1706277484296",
)

job.commit()
