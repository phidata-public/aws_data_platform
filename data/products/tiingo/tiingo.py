from phidata.product import DataProduct

from data.products.tiingo.load_tickers import tickers
from data.products.tiingo.load_prices import prices
from data.products.tiingo.load_etf_prices import etf_prices

##############################################################################
## This file defines a data product that contains workflows to:
##  1. Download daily list of stock tickers from tiingo
##  2. Download daily nasdaq 100 prices from tiingo
##  3. Download daily s&p 500 prices from tiingo
##  4. Download daily etf prices from tiingo
##############################################################################


tiingo = DataProduct(
    name="tiingo",
    workflows=[tickers, prices, etf_prices],
    graph={etf_prices: [tickers]},
)
dag = tiingo.create_airflow_dag(is_paused_upon_creation=True)
