#保存数据到数据库,每周需要保存的
from zvtm.domain import Stock1wkKdata
Stock1wkKdata.record_data(provider='joinquant', sleeping_time=0, day_data=True)