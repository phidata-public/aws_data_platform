from typing import Optional, Dict, Any, Union, List

from phidata.asset.table.sql.postgres import PostgresTable
from phidata.workflow import Workflow
from phidata.task import TaskArgs, task
from phidata.utils.log import logger

from data.products.tiingo.nasdaq_100 import nasdaq_100_tickers
from data.products.tiingo.sp_500 import sp_500_tickers
from workspace.config import dev_db, pg_db_connection_id

##############################################################################
## This file defines a workflow that downloads daily stock prices from tiingo
## The workflow is part of the tiingo data product
##############################################################################


# Step 1: Define a postgres table named `prices`.
prices_table = PostgresTable(
    name="daily_prices",
    db_conn_id=pg_db_connection_id,
    db_conn_url=dev_db.get_db_connection_url_local(),
)


# Step 2: Create the workflow which loads the prices_table
prices = Workflow(
    name="load_daily_prices",
    outputs=[prices_table],
)


# 2.1: Define typed inputs for our workflow
#   Create a class that inherits from TaskArgs
#   and contains the inputs for our task as class variables
class LoadPricesArgs(TaskArgs):
    # The prices table to load
    prices_table: PostgresTable = prices_table
    # Tickers to get prices for: as a string or list of strings
    tickers: Union[str, List[str]] = ["AAPL", "GOOG"]
    # start_date: Start of price download range in YYYY-MM-DD format.
    start_date: Optional[str] = None
    # end_date: End of price download range in YYYY-MM-DD format.
    end_date: Optional[str] = None
    # Rows to cache before writing to db
    cache_size: int = 5000
    frequency: str = "daily"
    # Tiingo Api key
    # We should set TIINGO_API_KEY as an env variable as this key should not be checked in
    # But for local testing, we can pass the api_key to this function if needed
    api_key: Optional[str] = None
    # If True, will drop table before loading data, thereby rewriting the table
    drop_table_before_load: bool = False


# 2.2: Write a task to drop daily data.
@prices.task()
def drop_daily_prices(**kwargs) -> bool:
    """
    This task drops daily data before loading, so we dont have duplicates
    """
    args = LoadPricesArgs.from_kwargs(kwargs)
    run_date = args.run_date

    # drop_table_before_load
    if args.drop_table_before_load:
        logger.info(f"Dropping table: {args.prices_table.name}")
        args.prices_table.delete()
    # or drop rows for current date so we dont have duplicates
    else:
        logger.info(f"Dropping data for: {run_date}")
        args.prices_table.run_sql_query(
            f"DELETE FROM {args.prices_table.name} WHERE ds = '{run_date}'"
        )
    return True


# 2.3: Write a task to load daily prices
# Instead of using the @workflow.task decorator, we will use the
# independent @task decorator, so we can reuse this function for multiple tickers.
# Note that there are no () when using the @task decorator
@task
def load_ticker_prices(**kwargs) -> bool:

    import pandas as pd
    from tiingo import TiingoClient

    # Get inputs as typed arguments by creating LoadPricesArgs from kwargs
    args: LoadPricesArgs = LoadPricesArgs.from_kwargs(kwargs)
    # logger.info(f"args: {args}")

    run_date = args.run_date
    if args.run_date is None:
        logger.error("Invalid run_date")
        return False
    logger.info(f"Loading {args.prices_table.name} for {run_date}")

    # Build tiingo_config
    tiingo_config: Dict[str, Any] = {
        "session": True,
    }
    # Set TIINGO_API_KEY if provided
    if args.api_key is not None:
        tiingo_config["api_key"] = args.api_key

    # Build TiingoClient
    tiingo_client: TiingoClient = TiingoClient(tiingo_config)

    # List of tickers to read prices for
    tickers_list: List[str] = []

    # if only 1 ticker is provided, convert to a list
    if isinstance(args.tickers, str):
        tickers_list = [args.tickers]
    elif isinstance(args.tickers, list):
        tickers_list = args.tickers

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
            logger.warning(
                f"Received error with {ticker}: {e}. Continuing to next ticker"
            )
            continue

        prices_df = prices_df.append(single_ticker_price)

        # write to table if rows_in_df > cache_size
        rows_in_df = prices_df.shape[0]
        if rows_in_df >= args.cache_size:
            prices_df["ds"] = run_date
            prices_df.reset_index(drop=True, inplace=True)
            prices_df.set_index(["ds", "ticker"], inplace=True)
            # prices_df.drop("date", axis=1, inplace=True)
            write_success = args.prices_table.write_pandas_df(
                prices_df, if_exists="append"
            )
            if write_success:
                # only clear existing df if write is successful
                prices_df = pd.DataFrame()

    # write final set of rows
    prices_df["ds"] = run_date
    prices_df.reset_index(drop=True, inplace=True)
    prices_df.set_index(["ds", "ticker"], inplace=True)
    # prices_df.drop("date", axis=1, inplace=True)

    # logger.info("Sample data:")
    # logger.info(prices_df[:5])
    return args.prices_table.write_pandas_df(prices_df, if_exists="append")


# 2.4: Instantiate the tasks
# Because we are reusing the load_ticker_prices task, we need to provide
#   a unique name so airflow can create separate tasks for each
# Run this:
#   Locally: `phi wf run tiingo:prices:nasdaq`
#   In a Dev databox: `phi wf run tiingo:prices:nasdaq -e dev`
#   In a Prd databox: `phi wf run tiingo:prices:nasdaq -e prd`
daily_nasdaq = load_ticker_prices(
    name="daily_nasdaq",
    tickers=nasdaq_100_tickers,
)
prices.add_task(daily_nasdaq)

# Run this:
#   Locally: `phi wf run tiingo:prices:sp500`
#   In a Dev databox: `phi wf run tiingo:prices:sp500 -e dev`
#   In a Prd databox: `phi wf run tiingo:prices:sp500 -e prd`
daily_sp500 = load_ticker_prices(
    name="daily_sp500",
    tickers=sp_500_tickers,
)
prices.add_task(daily_sp500)
