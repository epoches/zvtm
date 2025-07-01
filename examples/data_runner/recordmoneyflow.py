from examples.recorder_utils import run_data_recorder
from zvtm.domain import StockMoneyFlow
import datetime
data_provider="em"
run_data_recorder(domain=StockMoneyFlow, data_provider=data_provider,code='000672', sleeping_time=0,day_data = False,end_timestamp='2022-04-07') #,end_timestamp='2022-04-07',day_data = False,code='002932'
