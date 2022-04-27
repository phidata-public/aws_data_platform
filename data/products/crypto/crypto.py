from phidata.product import DataProduct

from data.products.crypto.metadata import metadata
from data.products.crypto.daily_prices import daily_prices

##############################################################################
## This file defines a data product that contains workflows to:
##  1. Download daily crypto metadata from tiingo
##  2. Download daily crypto prices from tiingo
##############################################################################


crypto = DataProduct(
    name="crypto",
    workflows=[metadata, daily_prices],
    graph={daily_prices: [metadata]},
)
dag = crypto.create_airflow_dag(is_paused_upon_creation=True)
