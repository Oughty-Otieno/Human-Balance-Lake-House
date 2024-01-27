import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node customer_landing
customer_landing_node1706165884929 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://eric-project-stedi/customer/landing/"],
        "recurse": True,
    },
    transformation_ctx="customer_landing_node1706165884929",
)

# Script generated for node Filter
Filter_node1706191338515 = Filter.apply(
    frame=customer_landing_node1706165884929,
    f=lambda row: (not (row["sharewithresearchasofdate"] == 0)),
    transformation_ctx="Filter_node1706191338515",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1706191440175 = glueContext.write_dynamic_frame.from_options(
    frame=Filter_node1706191338515,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://eric-project-stedi/customer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="CustomerTrusted_node1706191440175",
)

job.commit()
