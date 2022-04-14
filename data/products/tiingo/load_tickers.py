from typing import Optional, Dict, Any

from phidata.asset.table.sql.postgres import PostgresTable
from phidata.workflow import create_workflow, PythonWorkflowArgs
from phidata.utils.log import logger

from workspace.config import dev_db, pg_db_connection_id

##############################################################################
## This file defines a workflow that downloads daily stock tickers from tiingo
## The workflow is part of the tiingo data product
##############################################################################


# Step 1: Define a postgres table named `tickers`.
#   For local runs, use the connection url from dev_db.
#   For dev/prd use the pg_db_connection_id
tickers_table = PostgresTable(
    name="tickers",
    db_conn_id=pg_db_connection_id,
    db_conn_url=dev_db.get_db_connection_url_local(),
)


# Step 2: Create the workflow which loads the tickers_table
# 2.1: Define the inputs for our workflow
#   Create a class that inherits from PythonWorkflowArgs
#   and contains the inputs for our workflow as variables
class LoadTickersArgs(PythonWorkflowArgs):
    # The tickers table to load
    tickers_table: PostgresTable
    # Tiingo Api key
    # We should set TIINGO_API_KEY as an env variable, because this key should not be checked in
    # But for local testing, we can pass the api_key to this function
    api_key: Optional[str] = None
    # If True, will drop table before loading data, thereby rewriting the table
    drop_table_before_load: bool = False
    print_warning_on_no_data: bool = True


# 2.2: Create the workflow as a regular python function
@create_workflow
def load_tickers_table(**kwargs):
    import pandas as pd
    from tiingo import TiingoClient

    # Get inputs as typed arguments by creating LoadTickersArgs from kwargs
    args: LoadTickersArgs = LoadTickersArgs.from_kwargs(kwargs)
    # logger.info(f"LoadTickersArgs: {args}")
    run_date = args.run_date
    if args.run_date is None:
        logger.error("Invalid run_date")
        return False
    logger.info(f"Loading {args.tickers_table.name} for {run_date}")

    # Build tiingo_config
    tiingo_config: Dict[str, Any] = {
        "session": True,
    }
    # Access the inputs to this workflow as typed arguments
    # Set TIINGO_API_KEY if provided
    if args.api_key is not None:
        tiingo_config["api_key"] = args.api_key

    # Build TiingoClient
    tiingo_client: TiingoClient = TiingoClient(tiingo_config)

    tickers_df = tiingo_client.list_tickers(assetTypes=["Stock", "ETF", "Mutual Fund"])
    list_tickers_response = tiingo_client.list_stock_tickers()
    # logger.info("list_tickers_response type: {}".format(type(list_tickers_response)))
    # logger.info(f"list_tickers_response:\n{list_tickers_response[:5]}")

    tickers_df = pd.DataFrame(list_tickers_response)
    tickers_df["ds"] = run_date
    # tickers_df['ds'] = pd.Series([run_date for x in range(len(tickers_df.index))])
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

    num_tickers = len(tickers_df)
    logger.info(f"# tickers: {num_tickers}")
    logger.info("Sample data:")
    logger.info(tickers_df[:5])

    if num_tickers > 0:
        # drop_table_before_load
        if args.drop_table_before_load:
            args.tickers_table.delete()
        # or drop rows for current date so we dont have duplicates
        else:
            args.tickers_table.run_sql_query(f"DELETE * FROM {args.tickers_table.name} WHERE ds = '{run_date}'")

    return args.tickers_table.write_pandas_df(tickers_df, if_exists="append")


# Step 3: Instantiate the workflow which will be added to the tiingo data product
# Run this:
#   Locally: `phi wf run tiingo:load_ticker`
#   In a Dev databox: `phi wf run tiingo:load_ticker -e dev`
#   In a Prd databox: `phi wf run tiingo:load_ticker -e prd`
load_tickers = load_tickers_table(tickers_table=tickers_table, drop_table_before_load=True)
