from typing import Optional, Dict, Any, List

from phidata.asset.table.sql.postgres import PostgresTable
from phidata.workflow import Workflow
from phidata.task import TaskArgs, task
from phidata.utils.log import logger

from workspace.config import dev_db, pg_db_connection_id

##############################################################################
## This file defines a workflow that downloads crypto metadata from tiingo
## The workflow is part of the crypto data product
##############################################################################


# Step 1: Define a postgres table named `crypto_metadata`.
#   For testing locally, use the connection url from dev_db.
#   For dev/prd use the pg_db_connection_id
crypto_metadata = PostgresTable(
    name="crypto_metadata",
    db_conn_id=pg_db_connection_id,
    db_conn_url=dev_db.get_db_connection_url_local(),
)


# Step 2: Build a workflow that loads the crypto_metadata
# 2.1: Define the inputs for our workflow
#   Create a class that inherits from TaskArgs
#   and contains the input variables for our task as class variables
class LoadCryptoMetadataArgs(TaskArgs):
    # The table to load
    crypto_metadata: PostgresTable = crypto_metadata
    # If True, drop table before loading data and rewrite the table
    drop_table_before_load: bool = False


# 2.2: Write a task to drop daily data as a regular python function
@task
def drop_metadata(**kwargs) -> bool:
    """
    This task drops daily data before loading, so we dont have duplicates
    """
    args = LoadCryptoMetadataArgs.from_kwargs(kwargs)
    run_date = args.run_date

    # drop_table_before_load
    if args.drop_table_before_load:
        logger.info(f"Dropping table: {args.crypto_metadata.name}")
        args.crypto_metadata.delete()
    # or drop rows for current date so we dont have duplicates
    else:
        logger.info(f"Dropping data for: {run_date}")
        args.crypto_metadata.run_sql_query(
            f"DELETE FROM {args.crypto_metadata.name} WHERE ds = '{run_date}'"
        )
    return True


# 2.3: Instantiate the task that drops daily data
drop = drop_metadata()


# 2.4: Write a task to load daily data as a regular python function
@task
def load_metadata(**kwargs) -> bool:

    import pandas as pd
    from tiingo import TiingoClient

    # Get inputs as typed arguments by creating LoadCryptoMetadataArgs from kwargs
    args: LoadCryptoMetadataArgs = LoadCryptoMetadataArgs.from_kwargs(kwargs)

    run_date = args.run_date
    if args.run_date is None:
        logger.error("Invalid run_date")
        return False
    logger.info(f"Loading {args.crypto_metadata.name} for {run_date}")

    # Build tiingo_config
    tiingo_config: Dict[str, Any] = {
        "session": True,
    }
    # Build TiingoClient
    tiingo_client: TiingoClient = TiingoClient(tiingo_config)

    metadata_response = tiingo_client._request(
        "GET", url="tiingo/crypto", params={"format": "json"}
    )
    logger.debug(f"metadata_response.status_code: {metadata_response.status_code}")

    # metadata_list example:
    # [
    #     {
    #         "name": "Bitcoin Tether (BTC/USD)",
    #         "baseCurrency": "btc",
    #         "quoteCurrency": "usd",
    #         "ticker": "btcusd"
    #     }
    # ]
    metadata_list: Optional[List] = None
    if metadata_response.status_code == 200:
        metadata_list = metadata_response.json()
        # logger.info(f"type metadata_list: {type(metadata_list)}")
        # logger.info(f"metadata_list: {metadata_list[:5]}")
        if metadata_list is not None:
            logger.info(f"# tickers: {len(metadata_list)}")

    metadata_df: pd.DataFrame = pd.DataFrame(metadata_list)
    metadata_df["ds"] = run_date
    metadata_df.set_index(keys=["ds", "ticker"], inplace=True)
    metadata_df.rename(
        columns={
            "baseCurrency": "base_currency",
            "quoteCurrency": "quote_currency",
        },
        inplace=True,
    )

    logger.info("Sample data:")
    logger.info(metadata_df[:5])

    return args.crypto_metadata.write_pandas_df(metadata_df, if_exists="append")


# 2.5: Instantiate the task that loads daily data
load = load_metadata()


# 2.6: Create a Workflow object and add the tasks
metadata = Workflow(
    name="metadata",
    tasks=[drop, load],
    graph={
        load: [drop],
    },
    outputs=[crypto_metadata],
)
