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

# Script generated for node Accelorometer Trusted
AccelorometerTrusted_node1706277115574 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://eric-project-stedi/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="AccelorometerTrusted_node1706277115574",
)

# Script generated for node accelerometer Trusted
accelerometerTrusted_node1706277204144 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://eric-project-stedi/step_trainer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="accelerometerTrusted_node1706277204144",
)

# Script generated for node Join
AccelorometerTrusted_node1706277115574DF = AccelorometerTrusted_node1706277115574.toDF()
accelerometerTrusted_node1706277204144DF = accelerometerTrusted_node1706277204144.toDF()
Join_node1706277329493 = DynamicFrame.fromDF(
    AccelorometerTrusted_node1706277115574DF.join(
        accelerometerTrusted_node1706277204144DF,
        (
            AccelorometerTrusted_node1706277115574DF["timestamp"]
            == accelerometerTrusted_node1706277204144DF["sensorreadingtime"]
        ),
        "left",
    ),
    glueContext,
    "Join_node1706277329493",
)

# Script generated for node Drop Fields
DropFields_node1706449867088 = DropFields.apply(
    frame=Join_node1706277329493,
    paths=["user"],
    transformation_ctx="DropFields_node1706449867088",
)

# Script generated for node Amazon S3
AmazonS3_node1706277484296 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1706449867088,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://eric-project-stedi/machine learning/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="AmazonS3_node1706277484296",
)

job.commit()
