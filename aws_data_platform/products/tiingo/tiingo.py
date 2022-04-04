from typing import Optional, Dict, Any, List, Union

from phidata.asset.table.sql.postgres import PostgresTable
from phidata.product import DataProduct
from phidata.workflow import create_workflow, PythonWorkflowArgs
from phidata.workflow.run.sql.query import RunSqlQuery
from phidata.utils.log import logger

from aws_data_platform.workspace.config import dev_db, pg_db_connection_id

##############################################################################
## This data product download daily stock price data using the Tiingo Api
## Steps:
##  1. Get tickers
##  2. Get prices for each ticker
##############################################################################

# Step 1: Get tickers
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

    logger.info(f"sql_table: {sql_table}")
    logger.info(f"api_key: {api_key}")
    logger.info(f"kwargs: {kwargs}")

    # Build tiingo_config
    tiingo_config: Dict[str, Any] = {
        "session": True,
    }
    # Set TIINGO_API_KEY as an env variable since this key should not be checked in
    # But for local testing, we can pass the api_key to this function
    if api_key is not None:
        tiingo_config["api_key"] = api_key

    # Build TiingoClient
    tiingo_client: TiingoClient = TiingoClient(tiingo_config)

    tickers_df = tiingo_client.list_tickers(assetTypes=["Stock", "ETF", "Mutual Fund"])
    list_tickers_response = tiingo_client.list_stock_tickers()
    logger.info("list_tickers_response type: {}".format(type(list_tickers_response)))
    logger.info(f"list_tickers_response:\n{list_tickers_response[:5]}")

    tickers_df = pd.DataFrame(list_tickers_response)
    tickers_df.rename(
        columns={
            "assetType": "asset_type",
            "priceCurrency": "price_currency",
            "startDate": "start_date",
            "endDate": "end_date",
        },
        inplace=True,
    )
    logger.info("tickers_df:")
    logger.info(tickers_df[:5])
    logger.info("# tickers: {}".format(len(tickers_df)))

    return sql_table.write_pandas_df(tickers_df)


# Instantiate the Workflow that load the tickers table
load_tickers = load_tickers_table(
    tickers_table,
    "44178418bad2c63d651cd99d1354e194548e3fa2",
)


# Step 2: Get prices for each ticker
# Define a postgres table named `prices`.
prices_table = PostgresTable(
    name="prices",
    db_conn_id=pg_db_connection_id,
    db_conn_url=dev_db.get_db_connection_url_local(),
)


# To demo how we can add input validation for workflows
# Create a pydantic model containing the workflow args
class LoadTickerPricesArgs(PythonWorkflowArgs):
    # required: ticker or a list of tickers
    tickers: Union[str, List[str]]
    # required: PostgresTable to load
    sql_table: PostgresTable
    # start_date: Start of ticker range in YYYY-MM-DD format.
    start_date: Optional[str] = None
    # end_date: End of ticker range in YYYY-MM-DD format.
    end_date: Optional[str] = None
    frequency: str = "daily"
    response_format: str = "json"
    # valid options = {'open', 'high', 'low', 'close', 'volume',
    # 'adjOpen', 'adjHigh', 'adjLow', 'adjClose', 'adjVolume',
    # 'divCash', 'splitFactor'}
    metric: Optional[str] = None
    sort: Optional[str] = None
    columns: Optional[List[str]] = None
    use_session: bool = True
    # Set TIINGO_API_KEY as an env variable since this key should not be checked in
    # But for local testing, we can pass the api_key to this function
    api_key: Optional[str] = None


# Define a Workflow to load the tickers table
@create_workflow
def load_ticker_prices(**kwargs) -> bool:

    import pandas as pd
    from tiingo import TiingoClient

    args: LoadTickerPricesArgs = LoadTickerPricesArgs(**kwargs)
    logger.info("GetTickerPriceArgs: {}".format(args))

    # Build tiingo_config
    tiingo_config: Dict[str, Any] = {
        "session": True,
    }
    # Set TIINGO_API_KEY if provided
    if args.api_key is not None:
        tiingo_config["api_key"] = args.api_key

    # Build TiingoClient
    tiingo_client: TiingoClient = TiingoClient(tiingo_config)

    tickers = args.tickers
    ticker_price_df = pd.DataFrame()
    if isinstance(tickers, list):
        for ticker in tickers:
            _price_df: pd.DataFrame = tiingo_client.get_dataframe(
                tickers=ticker,
                startDate=args.start_date,
                endDate=args.end_date,
                frequency=args.frequency,
                fmt=args.response_format,
            )
            logger.info("_price_df:")
            logger.info(_price_df[:5])
            _price_df["ticker"] = ticker
            ticker_price_df = ticker_price_df.append(_price_df)
    else:
        ticker_price_df = tiingo_client.get_dataframe(
            tickers=tickers,
            startDate=args.start_date,
            endDate=args.end_date,
            frequency=args.frequency,
            fmt=args.response_format,
        )
        ticker_price_df["ticker"] = args.tickers

    ticker_price_df.reset_index(inplace=True)
    ticker_price_df.set_index(["date", "ticker"], inplace=True)
    logger.info("ticker_price_df:")
    logger.info(ticker_price_df[:5])
    logger.info("index: {}".format(ticker_price_df.index))

    return args.sql_table.write_pandas_df(ticker_price_df)


# Instantiate the Workflow that loads the prices table
load_prices = load_ticker_prices(
    tickers="GOOG",
    sql_table=prices_table,
    api_key="44178418bad2c63d651cd99d1354e194548e3fa2",
)

# Create a DataProduct for these tasks
tiingo = DataProduct(name="tiingo", workflows=[load_tickers, load_prices])
dag = tiingo.create_airflow_dag(
    is_paused_upon_creation=True
)
