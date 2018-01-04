import logging
import sys

from dataengineeringutils.s3 import delete_folder_from_bucket
from dataengineeringutils import glue
from dataengineeringutils.athena import make_partitions
from dataengineeringutils.s3 import upload_file_to_s3_from_path
import boto3

from pyathenajdbc import connect

s3_client = boto3.client('s3')

log = logging.getLogger(__name__)
logging.basicConfig(stream=sys.stdout, level=logging.WARNING)

logging.getLogger(__name__).setLevel(logging.DEBUG)

if __name__ == '__main__':

    glue.metadata_folder_to_database("metadata")

    local_path = "processed_data_for_upload/new_format/feb_2017_1.csv"
    s3_path = "raw_data/glue_error_data/year=2017/month=feb/data.csv"
    upload_file_to_s3_from_path(local_path, "alpha-dag-data-warehouse-template", s3_path)

    local_path = "processed_data_for_upload/new_format/mar_2017_1.csv"
    s3_path = "raw_data/glue_error_data/year=2017/month=mar/data.csv"
    upload_file_to_s3_from_path(local_path, "alpha-dag-data-warehouse-template", s3_path)

    # Build paritions
    temp_dir = 's3://alpha-dag-data-warehouse-template/temp_delete/'
    make_partitions("data_warehouse_template_raw", "glue_error_data", temp_dir)

    jobs_path = "s3://alpha-dag-data-warehouse-template/glue_fail_jobs/"
    glue.run_glue_job_from_local_folder_template("glue_jobs/job_makeall/", jobs_path, "glue_fail_example", "alpha_user_robinl")

