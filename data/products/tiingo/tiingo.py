from typing import Optional, Dict, Any, List, Union

from phidata.asset.table.sql.postgres import PostgresTable
from phidata.product import DataProduct
from phidata.workflow import create_workflow, PythonWorkflowArgs
from phidata.utils.log import logger

from workspace.config import dev_db, pg_db_connection_id

##############################################################################
## This data pipeline downloads daily stock price data using the Tiingo Api
## Steps:
##  1. Get tickers
##  2. Get prices for NASDAQ tickers
##############################################################################

# Define a postgres table named `tickers`.
# For local runs, use the connection url from dev_db. For dev/prd use the pg_db_connection_id
tickers_table = PostgresTable(
    name="tickers",
    db_conn_id=pg_db_connection_id,
    db_conn_url=dev_db.get_db_connection_url_local(),
)


# Define a Workflow to load the tickers table
@create_workflow
def load_tickers_table(sql_table: PostgresTable, api_key: Optional[str] = None, **kwargs):
    import pandas as pd
    from tiingo import TiingoClient

    # Build tiingo_config
    tiingo_config: Dict[str, Any] = {
        "session": True,
    }
    # You should set TIINGO_API_KEY as an env variable
    # But for local testing, we can pass the api_key to this function
    if api_key is not None:
        tiingo_config["api_key"] = api_key

    # Build TiingoClient
    tiingo_client: TiingoClient = TiingoClient(tiingo_config)

    tickers_df = tiingo_client.list_tickers(assetTypes=["Stock", "ETF", "Mutual Fund"])
    list_tickers_response = tiingo_client.list_stock_tickers()
    # logger.info("list_tickers_response type: {}".format(type(list_tickers_response)))
    # logger.info(f"list_tickers_response:\n{list_tickers_response[:5]}")

    tickers_df = pd.DataFrame(list_tickers_response)
    tickers_df.reset_index(drop=True, inplace=True)
    tickers_df.set_index("ticker", inplace=True)
    tickers_df.rename(
        columns={
            "assetType": "asset_type",
            "priceCurrency": "price_currency",
            "startDate": "start_date",
            "endDate": "end_date",
        },
        inplace=True,
    )
    logger.info("# tickers: {}".format(len(tickers_df)))
    logger.info("Sample data:")
    logger.info(tickers_df[:5])

    return sql_table.write_pandas_df(tickers_df, if_exists="replace")


# Define a postgres table named `prices`.
prices_table = PostgresTable(
    name="prices",
    db_conn_id=pg_db_connection_id,
    db_conn_url=dev_db.get_db_connection_url_local(),
)


# Adding input validation using pydantic
# Create a pydantic model with the parameters required by the
# workflow function
class LoadTickerPricesArgs(PythonWorkflowArgs):
    # required: prices_table to load
    prices_table: PostgresTable
    # required: tickers to get prices for
    # provide tickers as a string or list of strings
    tickers: Optional[Union[str, List[str]]] = None
    # provide tickers_table to read tickers from
    tickers_table: Optional[PostgresTable] = None
    # filter the tickers
    filter_asset_types: Optional[List[str]] = None
    filter_exchanges: Optional[List[str]] = None
    # start_date: Start of ticker range in YYYY-MM-DD format.
    start_date: Optional[str] = None
    # end_date: End of ticker range in YYYY-MM-DD format.
    end_date: Optional[str] = None
    frequency: str = "daily"
    use_session: bool = True
    # You should set TIINGO_API_KEY as an env variable
    # because this key should not be checked in
    # But for local testing, we can pass the api_key to this function
    api_key: Optional[str] = None
    chunksize: int = 1000
    print_warning_on_no_data: bool = True


# Define a Workflow to load the tickers table
@create_workflow
def load_ticker_prices(**kwargs) -> bool:
    import pandas as pd
    from tiingo import TiingoClient

    args: LoadTickerPricesArgs = LoadTickerPricesArgs.from_kwargs(kwargs)
    logger.info(f"GetTickerPriceArgs: {args}")

    if args.tickers is None and args.tickers_table is None:
        logger.error("Either tickers or tickers_table should be provided")
        return False

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

    # if tickers are provided manually, add them to tickers_list
    if args.tickers is not None:
        # if only 1 ticker is provided
        if isinstance(args.tickers, str):
            tickers_list.append(args.tickers)
        elif isinstance(args.tickers, list):
            tickers_list.extend(args.tickers)
    # otherwise read tickers_table
    elif args.tickers_table is not None:
        tickers_df: pd.DataFrame = args.tickers_table.read_pandas_df(columns=["ticker", "exchange", "asset_type"])
        if tickers_df.empty:
            logger.error(f"Table: {args.tickers_table.name} returned 0 rows")
            return False
        if args.filter_asset_types is not None:
            tickers_df = tickers_df.loc[tickers_df['asset_type'].isin(args.filter_asset_types)]
        if args.filter_exchanges is not None:
            tickers_df = tickers_df.loc[tickers_df['exchange'].isin(args.filter_exchanges)]
        tickers_list = tickers_df["ticker"].to_list()
    else:
        logger.error("Either tickers or tickers_table should be provided")
        return False

    # Get ticker prices and load prices_table
    ticker_prices = pd.DataFrame()
    if tickers_list is not None:
        for ticker in tickers_list:
            try:
                logger.info(f"Getting prices for {ticker}")
                single_ticker_price = tiingo_client.get_dataframe(
                    tickers=ticker,
                    startDate=args.start_date,
                    endDate=args.end_date,
                    frequency=args.frequency,
                )
                single_ticker_price["ticker"] = ticker

                ticker_prices = ticker_prices.append(single_ticker_price)

                # write to table if rows_in_df > chunksize
                rows_in_df = ticker_prices.shape[0]
                if rows_in_df >= args.chunksize:
                    logger.info(f"Writing {rows_in_df} rows")
                    ticker_prices.reset_index(inplace=True)
                    ticker_prices.set_index(["date", "ticker"], inplace=True)
                    write_success = args.prices_table.write_pandas_df(ticker_prices, if_exists="append")
                    if write_success:
                        ticker_prices = pd.DataFrame()
            except Exception:
                if args.print_warning_on_no_data:
                    logger.warning(f"Error getting price data for {ticker}")
                continue

        # write final set of rows
        ticker_prices.reset_index(inplace=True)
        ticker_prices.set_index(["date", "ticker"], inplace=True)
        logger.info("ticker_prices:")
        logger.info(ticker_prices[:5])
        return args.prices_table.write_pandas_df(ticker_prices, if_exists="append")

    return False


# Step 1: Get tickers
load_tickers = load_tickers_table(tickers_table)
# Step 2: Get prices for each ticker
load_prices = load_ticker_prices(
    prices_table=prices_table,
    tickers=["GOOG", "AAPL"],
    tickers_table=tickers_table,
    # filter_exchanges=["NASDAQ"],
    print_warning_on_no_data=False,
)

# Create a DataProduct for these tasks
tiingo = DataProduct(name="tiingo", workflows=[load_tickers, load_prices])
dag = tiingo.create_airflow_dag(
    is_paused_upon_creation=True
)
