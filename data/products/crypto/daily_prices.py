from datetime import datetime
from typing import Optional, Dict, Any, List

from pydantic import BaseModel
from phidata.asset.table.sql.postgres import PostgresTable
from phidata.workflow import Workflow
from phidata.task import TaskArgs, task
from phidata.utils.log import logger

from data.products.crypto.metadata import crypto_metadata
from workspace.config import dev_db, pg_db_connection_id

##############################################################################
## This file defines a workflow that downloads crypto prices from tiingo
## The workflow is part of the crypto data product
##############################################################################


# Step 1: Define a postgres table named `daily_crypto_prices`.
daily_crypto_prices = PostgresTable(
    name="daily_crypto_prices",
    db_conn_id=pg_db_connection_id,
    db_conn_url=dev_db.get_db_connection_url_local(),
)


# Step 2: Build a workflow that loads the daily_crypto_prices
# 2.1: Define typed inputs for our workflow
class LoadCryptoPricesArgs(TaskArgs):
    # The table to load
    daily_crypto_prices: PostgresTable = daily_crypto_prices
    # The metadata table to for crypto tickers
    crypto_metadata: PostgresTable = crypto_metadata
    # number of tickers per request
    tickers_per_request: int = 10
    # start_date: Start of price download range in YYYY-MM-DD format.
    start_date: Optional[str] = None
    # end_date: End of price download range in YYYY-MM-DD format.
    end_date: Optional[str] = None
    # Rows to cache before writing to db
    cache_size: int = 500
    # If True, will drop table before loading data, thereby rewriting the table
    drop_table_before_load: bool = False


# 2.2: Write a task to drop daily data.
@task
def drop_daily_prices(**kwargs) -> bool:
    """
    This task drops daily data before loading
    """
    args = LoadCryptoPricesArgs.from_kwargs(kwargs)
    run_date = args.run_date

    # drop_table_before_load
    if args.drop_table_before_load:
        logger.info(f"Dropping table: {args.daily_crypto_prices.name}")
        args.daily_crypto_prices.delete()
    # or drop rows for current date so we dont have duplicates
    else:
        logger.info(f"Dropping data for: {run_date}")
        args.daily_crypto_prices.run_sql_query(
            f"DELETE FROM {args.daily_crypto_prices.name} WHERE ds = '{run_date}'"
        )
    return True


# 2.3: Instantiate the task
drop = drop_daily_prices()


class CryptoPriceDataObject(BaseModel):
    date: Optional[datetime] = None
    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    close: Optional[float] = None
    tradesDone: Optional[int] = None
    volume: Optional[float] = None
    volumeNotional: Optional[float] = None


class CryptoPriceObject(BaseModel):
    ticker: str
    baseCurrency: Optional[str] = None
    quoteCurrency: Optional[str] = None
    priceData: List[CryptoPriceDataObject]


# 2.4: Write a task to load prices
@task
def load_daily_prices(**kwargs) -> bool:
    import pandas as pd
    from tiingo import TiingoClient

    # Get inputs as typed arguments by creating LoadCryptoPricesArgs from kwargs
    args: LoadCryptoPricesArgs = LoadCryptoPricesArgs.from_kwargs(kwargs)

    run_date = args.run_date
    if args.run_date is None:
        logger.error("Invalid run_date")
        return False
    logger.info(f"Loading {args.daily_crypto_prices.name} for {run_date}")

    # Build tiingo_config
    tiingo_config: Dict[str, Any] = {
        "session": True,
    }
    # Build TiingoClient
    tiingo_client: TiingoClient = TiingoClient(tiingo_config)

    # List of tickers to read prices for
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

    # Get prices
    prices_df: pd.DataFrame = pd.DataFrame()
    for idx in range(
        0, (num_tickers - args.tickers_per_request), args.tickers_per_request
    ):
        try:
            tickers = tickers_list[idx : (idx + args.tickers_per_request)]
            logger.info(f"Getting prices for {tickers}")

            # get ticker prices
            ticker_prices = tiingo_client.get_crypto_price_history(
                tickers=tickers,
                resampleFreq="1day",
            )
            # logger.info(f"type ticker_prices: {type(ticker_prices)}")
            # logger.info(f"ticker_prices: {ticker_prices}")

            # load prices_df with ticker_prices
            for ticker_price_data in ticker_prices:
                if not isinstance(ticker_price_data, dict):
                    logger.error(f"Skipping, Not a dict: {ticker_price_data}")
                    continue

                crypto_price_object = CryptoPriceObject(**ticker_price_data)
                # logger.info(f"type crypto_price_object: {type(crypto_price_object)}")
                # logger.info(f"crypto_price_object: {crypto_price_object}")

                ticker_price_rows = []
                for price_data in crypto_price_object.priceData:
                    price_data_row = {
                        "ds": run_date,
                        "ticker": crypto_price_object.ticker,
                        "base_currency": crypto_price_object.baseCurrency,
                        "quote_currency": crypto_price_object.quoteCurrency,
                        "datetime": price_data.date,
                        "open": price_data.open,
                        "high": price_data.high,
                        "low": price_data.low,
                        "close": price_data.close,
                        "trades_done": price_data.tradesDone,
                        "volume": price_data.volume,
                        "volume_notional": price_data.volumeNotional,
                    }
                    ticker_price_rows.append(price_data_row)

                ticker_price_df = pd.DataFrame(ticker_price_rows)
                prices_df = pd.concat([prices_df, ticker_price_df])

                # write to table if rows_in_df > cache_size
                rows_in_df = prices_df.shape[0]
                if rows_in_df >= args.cache_size:
                    prices_df.reset_index(drop=True, inplace=True)
                    prices_df.set_index(["ds", "ticker"], inplace=True)
                    write_success = args.daily_crypto_prices.write_pandas_df(
                        prices_df, if_exists="append"
                    )
                    if write_success:
                        # only clear existing df if write is successful
                        prices_df = pd.DataFrame()
        except Exception as e:
            logger.error(e)
            continue

    # write final set of rows
    prices_df.reset_index(drop=True, inplace=True)
    prices_df.set_index(["ds", "ticker"], inplace=True)

    logger.info("Sample data:")
    logger.info(prices_df[:10])
    return args.daily_crypto_prices.write_pandas_df(prices_df, if_exists="append")


# 2.5: Instantiate the task to load prices
load = load_daily_prices()

# 2.6: Create a Workflow object and add the tasks
daily_prices = Workflow(
    name="daily_prices",
    tasks=[drop, load],
    graph={
        load: [drop],
    },
    outputs=[daily_crypto_prices],
)
