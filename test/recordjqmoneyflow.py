from zvtm.domain import  StockMoneyFlow
# StockMoneyFlow.record_data(provider='joinquant', codes=['600689','000001'],sleeping_time=0)
# StockMoneyFlow.record_data(provider='joinquant', code='600689',sleeping_time=0)
StockMoneyFlow.record_data(provider='joinquant',sleeping_time=0,force_update=False)