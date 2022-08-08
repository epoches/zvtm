from zvtm.domain import  StockMoneyFlow,IndexMoneyFlow,BlockMoneyFlow,Block,BlockStock

BlockMoneyFlow.record_data(provider='sina', sleeping_time=1)
Block.record_data(provider='sina', sleeping_time=1)
#BlockStock.record_data(provider='sina', sleeping_time=1)
#IndexMoneyFlow.record_data(provider='joinquant', sleeping_time=0)
#StockMoneyFlow.record_data(provider='joinquant', sleeping_time=0)