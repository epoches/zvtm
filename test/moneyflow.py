from zvtm.domain import  StockMoneyFlow,IndexMoneyFlow,BlockMoneyFlow

#BlockMoneyFlow.record_data(provider='eastmoney', sleeping_time=0)
IndexMoneyFlow.record_data(provider='joinquant', sleeping_time=0)
#StockMoneyFlow.record_data(provider='joinquant', sleeping_time=0)