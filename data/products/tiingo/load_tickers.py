from typing import Optional, Dict, Any

from phidata.asset.table.sql.postgres import PostgresTable
from phidata.workflow import Workflow
from phidata.task import TaskArgs, task
from phidata.utils.log import logger

from workspace.config import dev_db, pg_db_connection_id

##############################################################################
## This file defines a workflow that downloads daily stock tickers from tiingo
## The workflow is part of the tiingo data product
##############################################################################


# Step 1: Define a postgres table named `daily_tickers`.
#   For testing locally, use the connection url from dev_db.
#   For dev/prd use the pg_db_connection_id
tickers_table = PostgresTable(
    name="daily_tickers",
    db_conn_id=pg_db_connection_id,
    db_conn_url=dev_db.get_db_connection_url_local(),
)


# Step 2: Build a workflow that loads the tickers_table
# 2.1: Define the inputs for our workflow
#   Create a class that inherits from TaskArgs
#   and contains the input variables for our task as class variables
class LoadTickersArgs(TaskArgs):
    # The table to load
    tickers_table: PostgresTable = tickers_table
    # Tiingo Api key should be provided using the TIINGO_API_KEY
    # env variable. But for local testing, we can pass the api_key here if needed
    api_key: Optional[str] = None
    # If True, drop table before loading data and rewrite the table
    drop_table_before_load: bool = False


# 2.2: Write a task to drop daily data as a regular python function
@task
def drop_tickers(**kwargs) -> bool:
    """
    This task drops daily data before loading, so we dont have duplicates
    """
    args = LoadTickersArgs.from_kwargs(kwargs)
    run_date = args.run_date

    # drop_table_before_load
    if args.drop_table_before_load:
        logger.info(f"Dropping table: {args.tickers_table.name}")
        args.tickers_table.delete()
    # or drop rows for current date so we dont have duplicates
    else:
        logger.info(f"Dropping data for: {run_date}")
        args.tickers_table.run_sql_query(
            f"DELETE FROM {args.tickers_table.name} WHERE ds = '{run_date}'"
        )
    return True


# 2.3: Instantiate the task that drops daily data
drop = drop_tickers()


# 2.4: Write a task to load daily tickers as a regular python function
@task
def load_tickers(**kwargs) -> bool:

    import pandas as pd
    from tiingo import TiingoClient

    # Get inputs as typed arguments by creating LoadTickersArgs from kwargs
    args: LoadTickersArgs = LoadTickersArgs.from_kwargs(kwargs)
    # logger.info(f"args: {args}")

    run_date = args.run_date
    if args.run_date is None:
        logger.error("Invalid run_date")
        return False
    logger.info(f"Loading {args.tickers_table.name} for {run_date}")

    # Build tiingo_config
    tiingo_config: Dict[str, Any] = {
        "session": True,
    }
    # Set TIINGO_API_KEY if provided
    if args.api_key is not None:
        tiingo_config["api_key"] = args.api_key

    # Build TiingoClient
    tiingo_client: TiingoClient = TiingoClient(tiingo_config)

    list_tickers_response = tiingo_client.list_tickers(
        assetTypes=["Stock", "ETF", "Mutual Fund"]
    )
    tickers_df: pd.DataFrame = pd.DataFrame(list_tickers_response)
    tickers_df["ds"] = run_date
    tickers_df.reset_index(drop=True, inplace=True)
    tickers_df.set_index(keys=["ds", "ticker"], inplace=True)
    tickers_df.rename(
        columns={
            "assetType": "asset_type",
            "priceCurrency": "price_currency",
            "startDate": "start_date",
            "endDate": "end_date",
        },
        inplace=True,
    )

    # num_tickers = len(tickers_df)
    # logger.info(f"# tickers: {num_tickers}")
    # logger.info("Sample data:")
    # logger.info(tickers_df[:5])

    return args.tickers_table.write_pandas_df(tickers_df, if_exists="append")


# 2.5: Instantiate the task that loads daily data
load = load_tickers()


# 2.6: Create a Workflow object and add the tasks
tickers = Workflow(
    name="tickers",
    tasks=[drop, load],
    graph={
        load: [drop],
    },
    outputs=[tickers_table],
)
