from phidata.asset.file import File
from phidata.asset.aws.s3 import S3Object
from phidata.asset.aws.glue.crawler import GlueCrawler, GlueS3Target
from phidata.asset.aws.athena.query import AthenaQuery
from phidata.product import DataProduct
from phidata.workflow.aws.athena.run_query import RunAthenaQuery
from phidata.workflow.aws.glue.create_crawler import CreateGlueCrawler
from phidata.workflow.upload.file.to_s3 import UploadFileToS3
from phidata.workflow.download.url.to_file import DownloadUrlToFile

from aws_data_platform.workspace.config import data_s3_bucket, glue_iam_role

##############################################################################
## This example shows how to build a data pipeline that calculates
## daily user count using s3, athena and glue.
## Steps:
##  1. Download user_activity data from a URL.
##  2. Upload user_activity data to a S3 bucket.
##  3. Create a glue crawler which creates our table
##  4. Run an athena query to calculate daily user count
##############################################################################

# Step 1: Download user_activity data from a URL.
# Define a File object which points to $WORKSPACE_DIR/storage/dau_aws/user_activity.csv
user_activity_csv = File(name="user_activity.csv", file_dir="dau_aws")
# Create a Workflow to download the user_activity data from a URL
download = DownloadUrlToFile(
    file=user_activity_csv,
    url="https://raw.githubusercontent.com/phidata-public/demo-data/main/dau_2021_10_01.csv",
)

# Step 2: Upload user_activity data to a S3 object.
# Define a S3 object for this file. Use the s3 bucket from the workspace config.
pipeline_name = "daily_active_users"
user_activity_s3 = S3Object(
    key=f"{pipeline_name}/ds_2021_10_01.csv",
    bucket=data_s3_bucket,
)
# Create a Workflow to upload the file downloaded above to our S3 object
upload = UploadFileToS3(
    file=user_activity_csv,
    s3_object=user_activity_s3,
)

# Step 3: Create a glue crawler for this table
# Define a GlueCrawler for the S3 object. Use the glue_iam_role from the workspace config.
database_name = "users"
table_name = pipeline_name
user_activity_crawler = GlueCrawler(
    name=f"{pipeline_name}-crawler",
    iam_role=glue_iam_role,
    database_name=database_name,
    s3_targets=[
        GlueS3Target(
            dir=table_name,
            bucket=data_s3_bucket,
        )
    ],
)
# Create a Workflow to create and start the crawler
crawler = CreateGlueCrawler(
    crawler=user_activity_crawler,
    start_crawler=True,
)

# Step 4: Run an athena query to calculate daily user count
user_count_query = AthenaQuery(
    name=f"{pipeline_name}-query",
    query_string=f"""
    SELECT
        ds,
        SUM(CASE WHEN is_active=1 THEN 1 ELSE 0 END) AS active_users
    FROM {database_name}.{table_name}
    GROUP BY ds
    ORDER BY ds
    """,
    database=database_name,
    output_location=f"s3://{data_s3_bucket.name}/queries/{database_name}/{table_name}/",
)
query = RunAthenaQuery(
    query=user_count_query,
    get_results=True,
)

# Create a DataProduct for these tasks
dau_aws = DataProduct(name="dau_aws", workflows=[download, upload, crawler, query])
dag = dau_aws.create_airflow_dag()
