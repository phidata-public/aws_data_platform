from phidata.product import DataProduct

from data.products.tiingo.load_tickers import load_tickers
from data.products.tiingo.load_prices import load_nasdaq, load_sp_500

##############################################################################
## This file defines a data product that contains workflows to:
##  1. Download daily list of stock tickers from tiingo
##  2. Download daily nasdaq 100 prices from tiingo
##  3. Download daily s&p 500 prices from tiingo
##############################################################################


tiingo = DataProduct(name="tiingo", workflows=[load_tickers, load_nasdaq, load_sp_500])
dag = tiingo.create_airflow_dag(is_paused_upon_creation=True)
