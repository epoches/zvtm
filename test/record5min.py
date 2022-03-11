#保存数据到数据库,5分钟需要保存的
from zvtm.domain import Stock5mHfqKdata


Stock5mHfqKdata.record_data(provider='joinquant',code='002382', sleeping_time=0,
   )#,code='000156',force_update=True,