from typing import Optional, Dict, Any, Union, List, Literal

from phidata.asset.table.sql.postgres import PostgresTable
from phidata.workflow import create_workflow, PythonWorkflowArgs
from phidata.utils.log import logger

from data.products.tiingo.nasdaq_100 import nasdaq_100
from data.products.tiingo.sp_500 import sp_500
from workspace.config import dev_db, pg_db_connection_id

##############################################################################
## This file defines a workflow that downloads daily stock prices from tiingo
## The workflow is part of the tiingo data product
##############################################################################


# Step 1: Define a postgres table named `prices`.
prices_table = PostgresTable(
    name="prices",
    db_conn_id=pg_db_connection_id,
    db_conn_url=dev_db.get_db_connection_url_local(),
)


# Step 2: Create the workflow which loads the prices_table
# 2.1: Define the inputs for our workflow
#   Create a class that inherits from PythonWorkflowArgs
#   and contains the inputs for our workflow as variables
class LoadPricesArgs(PythonWorkflowArgs):
    # required: prices_table to load
    prices_table: PostgresTable
    # required: tickers to get prices for
    # provide tickers as a string or list of strings
    tickers: Union[str, List[str]]
    # start_date: Start of price download range in YYYY-MM-DD format.
    start_date: Optional[str] = None
    # end_date: End of price download range in YYYY-MM-DD format.
    end_date: Optional[str] = None
    frequency: str = "daily"
    # We should set TIINGO_API_KEY as an env variable
    # because this key should not be checked in
    # But for local testing, we can pass the api_key to this function
    api_key: Optional[str] = None
    # Rows to cache before writing to db
    cache_size: int = 5000
    # If True, will drop table before loading data, thereby rewriting the table
    drop_table_before_load: bool = False
    print_warning_on_no_data: bool = True


# 2.2: Create the workflow as a regular python function
@create_workflow
def load_ticker_prices(**kwargs) -> bool:
    import pandas as pd
    from tiingo import TiingoClient

    # Get inputs as typed arguments by creating LoadPricesArgs from kwargs
    args: LoadPricesArgs = LoadPricesArgs.from_kwargs(kwargs)
    # logger.info(f"GetTickerPriceArgs: {args}")
    logger.info(f"Loading {args.prices_table.name} for {args.run_date}")

    if args.tickers is None:
        logger.error("Either tickers or tickers_table should be provided")
        return False

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

    # List of tickers to read prices for
    tickers_list: List[str] = []

    # if only 1 ticker is provided
    if isinstance(args.tickers, str):
        tickers_list = [args.tickers]
    elif isinstance(args.tickers, list):
        tickers_list = args.tickers

    # drop_table_before_load or drop rows for current date so we dont have duplicates
    # if args.drop_table_before_load:
    #     args.prices_table.run_sql_query(f"DROP TABLE {args.prices_table.name};")
    # else:
    #     args.prices_table.run_sql_query(f"DELETE * FROM {args.prices_table.name} WHERE ds = '{args.run_date}'")

    # Get ticker prices and load prices_table
    prices_df = pd.DataFrame()
    for ticker in tickers_list:
        logger.info(f"Getting prices for {ticker}")
        try:
            single_ticker_price = tiingo_client.get_dataframe(
                tickers=ticker,
                startDate=args.start_date,
                endDate=args.end_date,
                frequency=args.frequency,
            )
            single_ticker_price["ticker"] = ticker
        except Exception as e:
            logger.warning(f"Received error with {ticker}: {e}. Continuing to next ticker")
            continue

        prices_df = prices_df.append(single_ticker_price)

        # write to table if rows_in_df > cache_size
        rows_in_df = prices_df.shape[0]
        if rows_in_df >= args.cache_size:
            logger.info(f"Writing {rows_in_df} rows")
            prices_df["ds"] = args.run_date
            prices_df.reset_index(inplace=True)
            prices_df.set_index(["ds", "ticker"], inplace=True)
            prices_df.drop("date", axis=1, inplace=True)
            write_success = args.prices_table.write_pandas_df(
                prices_df, if_exists="append"
            )
            if write_success:
                # only clear existing df if write is successful
                prices_df = pd.DataFrame()

    # write final set of rows
    prices_df["ds"] = args.run_date
    prices_df.reset_index(inplace=True)
    prices_df.set_index(["ds", "ticker"], inplace=True)
    prices_df.drop("date", axis=1, inplace=True)
    logger.info("prices_df:")
    logger.info(prices_df[:5])
    return args.prices_table.write_pandas_df(prices_df, if_exists="append")


# Step 3: Instantiate the workflows which will be added to the tiingo data product
# Because we will be reusing the load_ticker_prices function, we will need to provide
# a unique task_id for each copy
# Run this:
#   Locally: `phi wf run tiingo:load_nasdaq`
#   In a Dev databox: `phi wf run tiingo:load_nasdaq -e dev`
#   In a Prd databox: `phi wf run tiingo:load_nasdaq -e prd`
load_nasdaq = load_ticker_prices(
    prices_table=prices_table,
    tickers=nasdaq_100,
    task_id="load_nasdaq_100",
)
# Run this:
#   Locally: `phi wf run tiingo:load_sp_500`
#   In a Dev databox: `phi wf run tiingo:load_sp_500 -e dev`
#   In a Prd databox: `phi wf run tiingo:load_sp_500 -e prd`
load_sp_500 = load_ticker_prices(
    prices_table=prices_table,
    tickers=sp_500,
    task_id="load_sp_500",
)
