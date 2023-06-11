import baostock as bs
import pandas as pd
from schedule.utils.mysql_pool import MysqlPool
from zvtm import init_log, zvt_config
from schedule.utils.time_utils import to_time_str,TIME_FORMAT_ISO8601
from datetime import datetime

#### 登陆系统 ####
lg = bs.login(user_id="anonymous", password="123456")
# 显示登陆返回信息
print('login respond error_code:' + lg.error_code)
print('login respond  error_msg:' + lg.error_msg)

#### 获取交易日信息 ####
rs = bs.query_trade_dates(start_date="2016-12-01", end_date="2023-12-31")
print('query_trade_dates respond error_code:' + rs.error_code)
print('query_trade_dates respond  error_msg:' + rs.error_msg)

#### 打印结果集 ####
data_list = []
while (rs.error_code == '0') & rs.next():
    # 获取一条记录，将记录合并在一起
    data_list.append(rs.get_row_data())
stock_data = pd.DataFrame(data_list, columns=rs.fields)
df = stock_data
df = df[df['is_trading_day'] == '1']
# print(df)
df = df[['calendar_date']]
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
    for item in arg['calendar_date']:
        # time = item[0].strftime('%Y-%m-%d %H:%M:%S')
        time = datetime.strptime(item, '%Y-%m-%d') #.strftime('%Y-%m-%d')
        id = "{}".format(to_time_str(time.strftime('%Y-%m-%d'), TIME_FORMAT_ISO8601))
        obj = [id,entity_id, time]
        values.append(obj)
    row = mp.insertmany(
        sql="insert into trade_day(id,entity_id,timestamp)values(%s,%s,%s)",
        args=values)

    # print(row)
    return row

insert_stock_orders(df)

#### 结果集输出到csv文件 ####
# df.to_csv("D:\\trade_datas.csv", encoding="gbk", index=False)
# print(df)

#### 登出系统 ####
bs.logout()