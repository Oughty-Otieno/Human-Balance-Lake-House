import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
import re
from pyspark.sql import functions as SqlFuncs

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
Join_node1706277259471 = Join.apply(
    frame1=accelerometer_node1706277204144,
    frame2=customertrusted_node1706277145132,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Join_node1706277259471",
)

# Script generated for node Join
Steplanding_node1706277115574DF = Steplanding_node1706277115574.toDF()
Join_node1706277259471DF = Join_node1706277259471.toDF()
Join_node1706277329493 = DynamicFrame.fromDF(
    Steplanding_node1706277115574DF.join(
        Join_node1706277259471DF,
        (
            Steplanding_node1706277115574DF["serialnumber"]
            == Join_node1706277259471DF["serialNumber"]
        ),
        "left",
    ),
    glueContext,
    "Join_node1706277329493",
)

# Script generated for node Filter
Filter_node1706374655720 = Filter.apply(
    frame=Join_node1706277329493,
    f=lambda row: (bool(re.match("^(?!\s*$).+", row["serialNumber"]))),
    transformation_ctx="Filter_node1706374655720",
)

# Script generated for node Drop Fields
DropFields_node1706375778597 = DropFields.apply(
    frame=Filter_node1706374655720,
    paths=[
        "z",
        "birthDay",
        "shareWithPublicAsOfDate",
        "shareWithResearchAsOfDate",
        "registrationDate",
        "customerName",
        "user",
        "shareWithFriendsAsOfDate",
        "y",
        "x",
        "timestamp",
        "email",
        "lastUpdateDate",
        "phone",
    ],
    transformation_ctx="DropFields_node1706375778597",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1706375950206 = DynamicFrame.fromDF(
    DropFields_node1706375778597.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1706375950206",
)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1706277484296 = glueContext.write_dynamic_frame.from_options(
    frame=DropDuplicates_node1706375950206,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://eric-project-stedi/step_trainer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="StepTrainerTrusted_node1706277484296",
)

job.commit()
