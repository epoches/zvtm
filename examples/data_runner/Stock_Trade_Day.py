from examples.recorder_utils import run_data_recorder
from zvtm.domain import StockTradeDay
data_provider="joinquant"
run_data_recorder(domain=StockTradeDay, data_provider=data_provider)