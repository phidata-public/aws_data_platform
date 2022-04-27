from phidata.asset.table.sql.postgres import PostgresTable
from phidata.task.run.sql.query import RunSqlQuery
from phidata.task.download.url.to_sql import DownloadUrlToSql
from phidata.workflow import Workflow

from workspace.config import dev_db, pg_db_connection_id

##############################################################################
## An example data pipeline that calculates daily active users using postgres.
## Steps:
##  1. Download user_activity data from a URL and load to a postgres table
##  2. Calculate daily active users and load results to a postgres table
##############################################################################

# Step 1: Download user_activity data from a URL and load to a postgres table
# Define a postgres table named `user_activity`. Use the connection url from dev_db in the workspace config.
user_activity_table = PostgresTable(
    name="user_activity",
    db_conn_id=pg_db_connection_id,
    db_conn_url=dev_db.get_db_connection_url_local(),
)

# Create a task that downloads the file and uploads to table
download = DownloadUrlToSql(
    name="download",
    url="https://raw.githubusercontent.com/phidata-public/demo-data/main/dau_2021_10_01.csv",
    sql_table=user_activity_table,
    if_exists="replace",
)

# Step 2: Calculate daily active users and load results to a postgres table
# Define a postgres table named `daily_active_users`.
daily_active_users_table = PostgresTable(
    name="daily_active_users",
    db_conn_id=pg_db_connection_id,
    db_conn_url=dev_db.get_db_connection_url_local(),
)

# Create a task that runs a SQL Query and loads the result to a table
load_dau = RunSqlQuery(
    name="load_dau",
    query=f"""
    SELECT
        ds,
        SUM(CASE WHEN is_active=1 THEN 1 ELSE 0 END) AS active_users
    FROM {user_activity_table.name}
    GROUP BY ds
    ORDER BY ds
    """,
    sql_table=user_activity_table,
    show_sample_data=True,
    load_result_to=daily_active_users_table,
    if_exists="replace",
)

# Create a workflow for these tasks
dau = Workflow(name="dau", tasks=[download, load_dau])
dag = dau.create_airflow_dag(is_paused_upon_creation=True)
