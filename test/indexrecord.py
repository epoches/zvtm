#指数数据
from zvtm.domain import Index,IndexStock,Index1dKdata


#Index.record_data(provider='exchange',code='000001')
#IndexStock.record_data(provider='exchange',code='000001')
#Index1dKdata.record_data(provider='em',code='000001')
#Index.record_data(provider='joinquant',code='000001')
#IndexStock.record_data(provider='joinquant',code='000001')
Index1dKdata.record_data(provider='joinquant',code='000001')
df=Index1dKdata.query_data(provider='joinquant',code='000001') #filters=[Index.category=='scope',Index.exchange='sh']
print(df)