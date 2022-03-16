#指数数据
from zvtm.domain import Index,IndexStock,Index1dKdata


Index.record_data(provider='exchange',code='000001')
IndexStock.record_data(provider='exchange',code='000001')
Index1dKdata.record_data(provider='em',code='000001')
#df=Index.query_data(filters=[Index.category=='scope',Index.exchange='sh'])
#print(df)