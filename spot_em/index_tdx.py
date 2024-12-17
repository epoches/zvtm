import pandas as pd
import requests
from tqdm import tqdm
import logging
from apscheduler.schedulers.background import BackgroundScheduler
from zvtm import init_log
import numpy as np
import pandas as pd
sched = BackgroundScheduler()
logger = logging.getLogger("__name__")
import datetime
from mootdx.quotes import Quotes
from sqlalchemy import create_engine, MetaData, Table, Column, String, Float, Integer, DateTime
from sqlalchemy.orm import sessionmaker
import pandas as pd
from zvtm.utils.time_utils import now_pd_timestamp, to_time_str, to_pd_timestamp
from zvtm import zvt_config
import platform
from struct import unpack
import os
client = Quotes.factory(market='std', multithread=True, heartbeat=True)
from sqlalchemy.dialects.mysql import insert

def UsePlatform():
    sysstr = platform.system()
    if(sysstr =="Windows"):
        # print ("Call Windows tasks")
        PATH = 'D:/new_jyplug/T0002/hq_cache/'
    elif(sysstr == "Linux"):
        # print ("Call Linux tasks")
        PATH = os.path.expanduser('~') + '/hq_cache/'
    return PATH
PATH = UsePlatform( )

def get_block_file(block='gn'):
    """  block_gn.dat,_fg.dat,_zs.dat  """

    file_name = f'block_{block}.dat'
    #print(PATH + file_name)
    with open(PATH + file_name, 'rb') as f:
        buff = f.read()

    head = unpack('<384sh', buff[:386])
    blk = buff[386:]
    blocks = [blk[i * 2813:(i + 1) * 2813] for i in range(head[1])]
    bk_list = []
    for bk in blocks:
        name = bk[:8].decode('gbk').strip('\x00')
        num, t = unpack('<2h', bk[9:13])
        stks = bk[13:(12 + 7 * num)].decode('gbk').split('\x00')
        bk_list = bk_list + [[name, block, num, stks]]
    return pd.DataFrame(bk_list, columns=['name', 'tp', 'num', 'stocks'])
# 读取本地文件
def read_file_loc(file_name, splits, encode='utf-8'):
    with open(file_name, 'r',encoding=encode) as f:
        buf_lis = f.read().split('\n')
    return [x.split(splits) for x in buf_lis[:-1]]


def get_block_zs_tdx_loc(block='hy'):

    buf_line = read_file_loc(PATH+'tdxzs3.cfg', '|', encode="GB2312")

    mapping = {'hy': '2', 'dq': '3', 'gn': '4', 'fg': '5', 'yjhy': '12', 'zs': '6'}
    df = pd.DataFrame(buf_line, columns=['name', 'code', 'type', 't1', 't2', 'block'])
    dg = df.groupby(by='type')
    #df.to_excel('block.xlsx')
    if (block == 'zs'):
        return df
    temp = dg.get_group(mapping[block]).reset_index(drop=True)
    temp.drop(temp.columns[[2, 3, 4]], axis=1, inplace=True)
    #temp.to_excel('tdxzs3.xlsx', index=False)
    return temp

# 读取概念板块
def gn_block(blk='gn') :
    bf = get_block_file(blk)
    t = get_block_zs_tdx_loc(blk)
    if (blk == 'zs'):
        return bf
    del t['block']
    #print(bf)
    #print(t)
    df = pd.merge(t,bf,on='name')
    #print(df)
    return df
# 获取通达信行业板块
def get_stock_hyblock_tdx_loc():
    buf_line = read_file_loc(PATH+'tdxhy.cfg', '|')
    buf_lis = []
    mapping = {'0': 'sz', '1': 'sh', '2': 'bj'}
    for x in buf_line:
        x[0] = mapping[x[0]]
        buf_lis.append(x)

    df = pd.DataFrame(buf_lis, columns=['c0', 'code', 'block', 'c1', 'c2', 'c3'])
    # print(df)
    df.drop(df.columns[[3, 4, 5]], axis=1, inplace=True)

    df = df[(df['block'] != '')]
    # df = df[df.code.str.startswith(('sz','sh'))]
    df['block5'] = df['block'].str[0:5]

    #df.to_excel('tdxhy.xlsx', index=False)
    return df

# 读取行业板块
def hy_block(blk='hy'):
    #begintime = datetime.datetime.now()
    stocklist = get_stock_hyblock_tdx_loc()
    #print(stocklist)
    blocklist = get_block_zs_tdx_loc(blk)
    #blocklist = blocklist.drop(blocklist[blocklist['name'].str.contains('TDX')].index)
    blocklist['block5'] = blocklist['block'].str[0:5]
    #print(blocklist)
    blocklist['num'] = 0
    blocklist['stocks'] = ''
    for i in range(len(blocklist)):
        blockkey = blocklist.iat[i, 2]
        if (len(blockkey) == 5):
            datai = stocklist[stocklist['block5'] == blockkey]  # 根据板块名称过滤
        else:
            datai = stocklist[stocklist['block'] == blockkey]  # 根据板块名称过滤
        # 板块内进行排序填序号
        datai = datai.sort_values(by=['code'], ascending=[True])
        codelist = datai['code'].tolist()
        blocklist.iat[i, 4] = len(codelist)
        blocklist.iat[i, 5] = str(codelist)
    blocklist = blocklist.drop(blocklist[blocklist['num'] == 0].index)
    return blocklist

from datetime import  timedelta
@sched.scheduled_job('cron',day_of_week='mon-fri', hour=20, minute=00)
def record_stock_data():
    # 获取entity_id 和 code
    df_block = get_block_zs_tdx_loc('hy')
    df_index = pd.DataFrame(columns=['open', 'close', 'high', 'low', 'vol', 'amount', 'year', 'month', 'day',
       'hour', 'minute', 'datetime', 'up_count', 'down_count', 'volume',
       'code', 'entity', 'entity_id', 'timestamp', 'id'])
    for i in tqdm(range(len(df_block))):
        code = df_block['code'].iloc[i]
        name = df_block['name'].iloc[i]
        entity = 'hy'#df_block['c0'].iloc[i]
        # code = '881241'
        # entity = 'sh'
        df = client.index(symbol=code, frequency=4, start=0, offset=1)
        if len(df) == 0:
            continue
        # if i ==4:
        #     break
        df['code'] = code
        df['name'] = name
        df['entity'] = entity
        df['entity_id'] = ""
        df = df.reset_index(drop=True)
        df['timestamp'] = pd.to_datetime(df['datetime'])
        for i in range(len(df)):
            entity_id = "{}_{}_{}".format('index', df["entity"].iloc[i], df["code"].iloc[i])
            df.at[i,"entity_id"] = entity_id
            df.at[i,"id"] = "{}_{}".format(to_time_str(entity_id), df["timestamp"].iloc[i].strftime('%Y-%m-%d'))
        frames=[df_index,df]
        df_index = pd.concat(frames, ignore_index=True)
    DATABASE_URI = 'mysql+pymysql://{}:{}@{}:{}/{}'.format(
        zvt_config['mysql_user'], zvt_config['mysql_password'],
        zvt_config['mysql_host'], zvt_config['mysql_port'], 'tdx_index_1d_kdata'
    )
    cols = ['id', 'code','name', 'entity_id', 'timestamp', 'open', 'close', 'high', 'low', 'amount', 'volume']
    df_index = df_index[cols]
    print(len(df_index))
    # 创建数据库引擎
    engine = create_engine(DATABASE_URI, echo=True)
    # 创建会话类
    Session = sessionmaker(bind=engine)
    # 创建会话
    session = Session()

    # 假设 df 是您的 pandas DataFrame
    # 将日期列转换为 datetime 并格式化
    # df['datetime'] = pd.to_datetime(df['datetime'])

    # 定义数据库元数据
    metadata = MetaData()

    # 定义表结构，这里需要根据实际的数据库表结构进行修改
    stock_table = Table(
        'index_1d_kdata', metadata,
        Column('id', String(128), primary_key=True),
        Column('code', String(20)),
        Column('name', String(60)),
        Column('entity_id', String(20)),
        Column('open', Float),
        Column('close', Float),
        Column('high', Float),
        Column('low', Float),
        # Column('vol', Float),  # 假设 vol 是交易量，使用 Float 类型
        Column('amount', Float),  # 假设 amount 是交易金额，使用 Float 类型
        # 其他列根据需要添加
        Column('timestamp', DateTime),
        Column('volume', Float),  # 假设 volume 是成交量，使用 Float 类型
        # 可以根据需要添加主键或其他约束
    )

    # 如果表不存在则创建表
    metadata.create_all(engine)

    try:
        # 将 DataFrame 转换为可以批量插入的格式
        records = df_index.to_dict('records')
        for record in records:
            # 创建一个 insert() 语句
            stmt = (
                insert(stock_table).
                    values(**record).
                    on_duplicate_key_update(
                    code=record['code'],
                    name=record['name'],
                    entity_id=record['entity_id'],
                    open=record['open'],
                    close=record['close'],
                    high=record['high'],
                    low=record['low'],
                    amount=record['amount'],
                    timestamp=record['timestamp'],
                    volume=record['volume'],
                )
            )
            # 执行语句
            session.execute(stmt)

        # 提交事务
        session.commit()
    except Exception as e:
        # 如果发生错误，回滚事务
        session.rollback()
        print(f"An error occurred: {e}")
    finally:
        # 关闭会话
        session.close()

if __name__ == "__main__":

    init_log("index_tdx.log")

    record_stock_data()

    # sched.start()
    #
    # sched._thread.join()