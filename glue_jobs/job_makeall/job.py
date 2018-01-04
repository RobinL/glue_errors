"""
This job demonstrates how to achieve a number of different
commonly needed actions in glue:

- Access functions in the resources specified in the glue job (.py, .sql files etc)
- Download a file from a URL and load into spark
- Convert a plain text dataset from the Glue catalogue into an RDD, so it can then be parsed


See the pyspark docs here: http://spark.apache.org/docs/2.2.0/api/python/pyspark.sql.html#
"""

import os
import sys
import urllib
import csv
# from awsglue.transforms import *
# from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.types import StringType, IntegerType, StructField, StructType
from pyspark import SparkFiles
from awsglue.context import DynamicFrame

# This should successfully import a custom function from our custom code in glue_py_resources/test_python_lib
from test_python_lib import get_two

# This enables logs to be written to cloudwatch
import logging
log = logging.getLogger(__name__)
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

sc = SparkContext()

glueContext = GlueContext(sc)
spark = glueContext.spark_session

# So we can verify from the logs that the various resources (.py files etc) were successfully imported
log.debug("The cwd is {}".format(os.getcwd()))
log.debug("The files in the directory are {}".format(os.listdir(".")))

# Load ina csv 'custom resource' that was uploaded from s3
customSchema = StructType([StructField("code", StringType(), True),
                           StructField("value", StringType(), True)])
# See https://stackoverflow.com/questions/24735516/spark-how-to-use-sparkcontext-textfile-for-local-file-system


csv_path = os.path.join(os.getcwd(), "category_lookup.csv")

log.debug("Before read.csv")

lookup = spark.read.csv(r"file://" + csv_path, schema = customSchema, header = True)
lookup.registerTempTable('lookup')
log.debug("After register temp table")


new_format = glueContext.create_dynamic_frame.from_catalog(
             database="data_warehouse_template_raw",
             table_name="glue_error_data")
new_format = new_format.toDF()
new_format.registerTempTable('glue_error_data')


# demonstrate opening a sql file using the custom function provided in test_python_lib.py
sql = """
select glue_error_data.*, lookup.value as lookup_value
from glue_error_data
left join lookup
on glue_error_data.cat = lookup.code
"""

joined = spark.sql(sql)
joined.registerTempTable('joined')

this_table = joined.collect()

log.debug("this table contains {}".format(this_table))
