from zvtm.domain import Stock,StockDetail
Stock.record_data(provider='eastmoney',sleeping_time=0)
StockDetail.record_data(provider='eastmoney',sleeping_time=0)
# Stock.record_data(provider='em',sleeping_time=0)
# StockDetail.record_data(provider='em',sleeping_time=0)