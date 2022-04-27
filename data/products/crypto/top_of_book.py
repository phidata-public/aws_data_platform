from typing import Optional, Dict, Any, List

from phidata.asset.table.sql.postgres import PostgresTable
from phidata.workflow import Workflow
from phidata.task import TaskArgs, task
from phidata.utils.log import logger

from data.products.crypto.metadata import crypto_metadata
from workspace.config import dev_db, pg_db_connection_id

##############################################################################
## This file defines a workflow that downloads crypto top of book data from tiingo
## The workflow is part of the crypto data product
##############################################################################


# Step 1: Define a postgres table named `crypto_top_of_book`.
crypto_top_of_book = PostgresTable(
    name="crypto_top_of_book",
    db_conn_id=pg_db_connection_id,
    db_conn_url=dev_db.get_db_connection_url_local(),
)


# Step 2: Build a workflow that loads the crypto_top_of_book
# 2.1: Define typed inputs for our workflow
class LoadCryptoPricesArgs(TaskArgs):
    # The table to load
    crypto_top_of_book: PostgresTable = crypto_top_of_book
    # The metadata table to for crypto tickers
    crypto_metadata: PostgresTable = crypto_metadata
    # number of tickers per request
    tickers_per_request: int = 10
    # Rows to cache before writing to db
    cache_size: int = 5000
    # If True, will drop table before loading data, thereby rewriting the table
    drop_table_before_load: bool = False


# 2.2: Write a task to drop daily data.
@task
def drop_top_of_book(**kwargs) -> bool:
    """
    This task drops daily data before loading
    """
    args = LoadCryptoPricesArgs.from_kwargs(kwargs)
    run_date = args.run_date

    # drop_table_before_load
    if args.drop_table_before_load:
        logger.info(f"Dropping table: {args.crypto_top_of_book.name}")
        args.crypto_top_of_book.delete()
    # or drop rows for current date so we dont have duplicates
    else:
        logger.info(f"Dropping data for: {run_date}")
        args.crypto_top_of_book.run_sql_query(
            f"DELETE FROM {args.crypto_top_of_book.name} WHERE ds = '{run_date}'"
        )
    return True


# 2.3: Instantiate the task
drop = drop_top_of_book()


# 2.4: Write a task to load top of book data
@task
def load_top_of_book(**kwargs) -> bool:

    import pandas as pd
    from tiingo import TiingoClient

    # Get inputs as typed arguments by creating LoadCryptoPricesArgs from kwargs
    args: LoadCryptoPricesArgs = LoadCryptoPricesArgs.from_kwargs(kwargs)

    run_date = args.run_date
    if args.run_date is None:
        logger.error("Invalid run_date")
        return False
    logger.info(f"Loading {args.crypto_top_of_book.name} for {run_date}")

    # Build tiingo_config
    tiingo_config: Dict[str, Any] = {
        "session": True,
    }
    # Build TiingoClient
    tiingo_client: TiingoClient = TiingoClient(tiingo_config)

    # List of tickers to read top of book data for
    crypto_tickers: pd.DataFrame = args.crypto_metadata.run_sql_query(
        sql_query=f"""
        SELECT ticker
        FROM {args.crypto_metadata.name}
        WHERE
            ds = '{run_date}'
            AND name is not null
        """,
    )
    tickers_list = list(crypto_tickers["ticker"])
    num_tickers = len(tickers_list)
    logger.info(f"# Tickers: {num_tickers}")

    # Get top of book data
    top_of_book_df: pd.DataFrame = pd.DataFrame()
    for idx in range(
        0, (num_tickers - args.tickers_per_request), args.tickers_per_request
    ):
        logger.info(f"idx: {idx}")
        tickers = tickers_list[idx : (idx + args.tickers_per_request)]
        logger.info(f"Getting top of book data for {tickers}")
        try:
            ticker_top_of_book = tiingo_client.get_crypto_top_of_book(
                tickers=tickers,
                includeRawExchangeData=True,
            )
            logger.info(f"type ticker_top_of_book: {type(ticker_top_of_book)}")
            logger.info(f"ticker_top_of_book: {ticker_top_of_book}")
            # ticker_top_of_book["ticker"] = ticker
        except Exception as e:
            logger.error(e)
            continue

        top_of_book_df = pd.concat([top_of_book_df, ticker_top_of_book])

    #     # write to table if rows_in_df > cache_size
    #     rows_in_df = top_of_book_df.shape[0]
    #     if rows_in_df >= args.cache_size:
    #         top_of_book_df["ds"] = run_date
    #         top_of_book_df.reset_index(drop=True, inplace=True)
    #         top_of_book_df.set_index(["ds", "ticker"], inplace=True)
    #         write_success = args.crypto_top_of_book.write_pandas_df(
    #             top_of_book_df, if_exists="append"
    #         )
    #         if write_success:
    #             # only clear existing df if write is successful
    #             top_of_book_df = pd.DataFrame()
    #
    # # write final set of rows
    # top_of_book_df["ds"] = run_date
    # top_of_book_df.reset_index(drop=True, inplace=True)
    # top_of_book_df.set_index(["ds", "ticker"], inplace=True)
    #
    logger.info("Sample data:")
    logger.info(top_of_book_df[:5])
    return args.crypto_top_of_book.write_pandas_df(top_of_book_df, if_exists="append")


# 2.5: Instantiate the task to load top of book data
load = load_top_of_book()

# 2.6: Create a Workflow object and add the tasks
top_of_book = Workflow(
    name="top_of_book",
    tasks=[drop, load],
    graph={
        load: [drop],
    },
    outputs=[crypto_top_of_book],
)
