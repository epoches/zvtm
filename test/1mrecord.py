# 板块和大盘1分钟线更新
from zvtm.domain import Block1mKdata,Index1mKdata

Block1mKdata.record_data(provider="em")
Index1mKdata.record_data(provider='joinquant',code='000001')
