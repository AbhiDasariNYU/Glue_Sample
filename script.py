import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

## Data Catalog: database and table name
db_name = 'employee_db'
tbl_name = 'employee_info'

## Read data from Glue Data Catalog
dynamic_frame_read = glueContext.create_dynamic_frame.from_catalog(database = db_name, table_name = tbl_name)

## Transformations, if needed
# dynamic_frame_transformed = ApplyMapping.apply(frame = dynamic_frame_read, mappings = [("col_name", "string", "col_name", "string")])

## Write out to S3
output_dir = "s3://<your_output_bucket>/output/"
dynamic_frame_read.toDF().write.format("parquet").save(output_dir)

job.commit()
