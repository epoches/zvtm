import tushare as ts
from schedule.utils.mysql_pool import MysqlPool
from zvtm import init_log, zvt_config
from schedule.utils.time_utils import to_time_str,TIME_FORMAT_ISO8601
from datetime import datetime


token = 'd36d566f61ff364b5b6824484426752e1bbbe092dfaaec2d9c2cd996'
pro = ts.pro_api(token)


df = pro.trade_cal(exchange='', start_date='20240101', end_date='20241231')
# print(df)
df = df[df['is_open'] == 1]
# print(df)
df = df['cal_date']
print(df)
def insert_stock_orders(arg):
    db = 'tushare'
    mp = MysqlPool(zvt_config['mysql_host'], zvt_config['mysql_user'], zvt_config['mysql_password'], db,
                   int(zvt_config['mysql_port']), 'utf8')

    # 插入数据
    # id = "{}".format(to_time_str(dt, TIME_FORMAT_ISO8601)
    #                     )
    # time = dt.strftime('%Y-%m-%d %H:%M:%S')


    # data = mp.insert("insert into trade_day(id,entity_id,timestamp)values(%s,%s,%s)", (id,entity_id,time))
    values = []
    entity_id = 'stock_sz_000001'
    for item in arg:

        # time = item[0].strftime('%Y-%m-%d %H:%M:%S')
        time = datetime.strptime(item, '%Y%m%d') #.strftime('%Y-%m-%d')
        id = "{}".format(to_time_str(time.strftime('%Y-%m-%d'), TIME_FORMAT_ISO8601))
        obj = [id,entity_id, time]
        values.append(obj)
    row = mp.insertmany(
        sql="insert into trade_day(id,entity_id,timestamp)values(%s,%s,%s)",
        args=values)

    print(row)
    return row

insert_stock_orders(df)
