import tushare as ts
from schedule.utils.mysql_pool import MysqlPool
from zvtm import init_log, zvt_config
from schedule.utils.time_utils import to_time_str,TIME_FORMAT_ISO8601
from datetime import datetime
token = '94cf551086fb5f314fc47376b7332c0c852f59dca499d70aaacffc2d'
pro = ts.pro_api(token)
# 000001.SH
df = pro.index_daily(ts_code='399300.SZ', start_date='20180101', end_date='20181010')
print(df)
