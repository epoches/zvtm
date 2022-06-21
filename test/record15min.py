#保存数据到数据库,5分钟需要保存的
from zvtm.domain import Stock15mHfqKdata

Stock15mHfqKdata.record_data(provider='em', sleeping_time=1)
# Stock15mHfqKdata.record_data(provider='joinquant',code='000156', sleeping_time=0,
#     start_timestamp = '2015-02-11', end_timestamp = '2015-2-17')#,code='000156',force_update=True