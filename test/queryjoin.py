#对stock和stockdetail联合查询
from zvtm.domain import Stock1dHfqKdata,Stock,StockDetail
Stock.query_data(provider='joinquant',code='000001')