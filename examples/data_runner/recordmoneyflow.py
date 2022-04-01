from examples.recorder_utils import run_data_recorder
from zvtm.domain import StockMoneyFlow
data_provider="joinquant"
run_data_recorder(domain=StockMoneyFlow, data_provider=data_provider, sleeping_time=0,day_data = False) #,code='002932'
