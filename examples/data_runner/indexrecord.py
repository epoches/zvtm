#指数数据
from zvtm.domain import Index1dKdata
from examples.recorder_utils import run_data_recorder
run_data_recorder(
        domain=Index1dKdata, data_provider="joinquant", entity_provider="joinquant",code='000001', day_data=False
    )
df=Index1dKdata.query_data(provider='joinquant',code='000001') #filters=[Index.category=='scope',Index.exchange='sh']
print(df)