"""
东方财富-数据中心-年报季报-业绩预告
https://data.eastmoney.com/bbsj/202003/yjyg.html
"""
import pandas as pd
import requests
from tqdm import tqdm


def stock_yjyg_em(date: str = "20200331") -> pd.DataFrame:
    """
    东方财富-数据中心-年报季报-业绩预告
    https://data.eastmoney.com/bbsj/202003/yjyg.html
    :param date: "2020-03-31", "2020-06-30", "2020-09-30", "2020-12-31"; 从 2008-12-31 开始
    :type date: str
    :return: 业绩预告
    :rtype: pandas.DataFrame
    """
    url = "https://datacenter.eastmoney.com/securities/api/data/v1/get"
    params = {
        "sortColumns": "NOTICE_DATE,SECURITY_CODE",
        "sortTypes": "-1,-1",
        "pageSize": "500",
        "pageNumber": "1",
        "reportName": "RPT_PUBLIC_OP_NEWPREDICT",
        "columns": "ALL",
        "filter": f" (REPORT_DATE='{'-'.join([date[:4], date[4:6], date[6:]])}')",
    }
    r = requests.get(url, params=params)
    data_json = r.json()
    big_df = pd.DataFrame()
    if data_json["code"] == 9201:
        return pd.DataFrame()
    total_page = data_json["result"]["pages"]
    for page in tqdm(range(1, total_page + 1), leave=False):
        params.update(
            {
                "pageNumber": page,
            }
        )
        r = requests.get(url, params=params)
        data_json = r.json()
        temp_df = pd.DataFrame(data_json["result"]["data"])
        big_df = pd.concat([big_df, temp_df], ignore_index=True)

    big_df.reset_index(inplace=True)
    big_df["index"] = range(1, len(big_df) + 1)
    big_df.columns = [
        "序号",
        "_",
        "股票代码",
        "股票简称",
        "_",
        "公告日期",
        "报告日期",
        "_",
        "预测指标",
        "_",
        "_",
        "_",
        "_",
        "业绩变动",
        "业绩变动原因",
        "预告类型",
        "上年同期值",
        "_",
        "_",
        "_",
        "_",
        "业绩变动幅度",
        "预测数值",
        "_",
        "_",
        "_",
        "_",
        "_",
    ]
    big_df = big_df[
        [
            "序号",
            "股票代码",
            "股票简称",
            "预测指标",
            "业绩变动",
            "预测数值",
            "业绩变动幅度",
            "业绩变动原因",
            "预告类型",
            "上年同期值",
            "公告日期",
        ]
    ]
    big_df["公告日期"] = pd.to_datetime(big_df["公告日期"]).dt.date
    big_df["业绩变动幅度"] = pd.to_numeric(big_df["业绩变动幅度"], errors="coerce")
    big_df["预测数值"] = pd.to_numeric(big_df["预测数值"], errors="coerce")
    big_df["上年同期值"] = pd.to_numeric(big_df["上年同期值"], errors="coerce")
    return big_df

import logging
from apscheduler.schedulers.background import BackgroundScheduler
from zvtm import init_log
import numpy as np
import pandas as pd
sched = BackgroundScheduler()
logger = logging.getLogger("__name__")
import datetime
from datetime import  timedelta
@sched.scheduled_job('cron',day_of_week='mon-fri', hour=23, minute=10)
def record_stock_data():
    now = datetime.datetime.now()
    month = (now.month - 1) - (now.month - 1) % 3 + 1
    this_quarter_start = datetime.datetime(now.year, month, 1)
    # 上季第一天和最后一天
    last_quarter_end = this_quarter_start - timedelta(days=1)
    last_quarter_end = last_quarter_end.strftime("%Y%m%d")
    stock_yjyg_em_df = stock_yjyg_em(date=last_quarter_end)
    print(stock_yjyg_em_df)
    if len(stock_yjyg_em_df) == 0:
        logger.info("上季度没有数据")
        return False
    stock_yjyg_em_df['公告日期'] = pd.to_datetime(stock_yjyg_em_df['公告日期'])
    stock_yjyg_em_df['公告日期'] = stock_yjyg_em_df['公告日期'].dt.strftime('%Y-%m-%d')
    stock_yjyg_em_df['序号'] = stock_yjyg_em_df['股票代码'] + '_' + stock_yjyg_em_df['公告日期']

    stock_yjyg_em_df = stock_yjyg_em_df.fillna(
        value=np.nan)  # Make sure all NaNs are explicitly set if they weren't already
    # Replace NaN with a default value, e.g., 0
    stock_yjyg_em_df.replace({np.nan: 0}, inplace=True)
    stock_yjyg_em_df['业绩变动原因'].replace({0: '无'}, inplace=True)
    stock_yjyg_em_df['业绩变动原因'] = stock_yjyg_em_df['业绩变动原因'].apply(lambda x: x.encode('utf-8'))

    from sqlalchemy import create_engine, MetaData, Table, Column, String, Float, BLOB, Date
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy.dialects.mysql import insert
    from sqlalchemy.exc import SQLAlchemyError
    from zvtm import zvt_config
    DATABASE_URI = 'mysql+pymysql://'+ zvt_config['mysql_user'] +':' +zvt_config['mysql_password'] + '@' +zvt_config['mysql_host'] + ':' + zvt_config['mysql_port']  +'/eastmoney_finance'

    engine = create_engine(DATABASE_URI, echo=True)
    Session = sessionmaker(bind=engine)
    metadata = MetaData()

    # 定义表结构
    em_yjyg = Table(
        'em_yjyg', metadata,
        Column('序号', String(128), primary_key=True),
        Column('股票代码', String(20)),
        Column('股票简称', String(50)),
        Column('预测指标', String(100)),
        Column('业绩变动', String(1500)),
        Column('预测数值', Float),
        Column('业绩变动幅度', Float),
        Column('业绩变动原因', BLOB),
        Column('预告类型', String(50)),
        Column('上年同期值', Float),
        Column('公告日期', Date)
    )

    metadata.create_all(engine)  # 如果表不存在则创建表
    session = Session()


    try:
        for index, row in stock_yjyg_em_df.iterrows():
            # 创建一个 insert() 语句，如果遇到主键冲突则更新记录
            stmt = (
                insert(em_yjyg).values({
                    '序号': row['序号'],
                    '股票代码': row['股票代码'],
                    '股票简称': row['股票简称'],
                    '预测指标': row['预测指标'],
                    '业绩变动': row['业绩变动'],
                    '预测数值': row['预测数值'],
                    '业绩变动幅度': row['业绩变动幅度'],
                    '业绩变动原因': row['业绩变动原因'],
                    '预告类型': row['预告类型'],
                    '上年同期值': row['上年同期值'],
                    '公告日期': row['公告日期']
                })
                    .on_duplicate_key_update(
                    {  # 使用字典指定更新的列
                        '股票代码': row['股票代码'],
                        '股票简称': row['股票简称'],
                        # ... 其他列
                        # 确保所有列都包括在内，除了主键 '序号'
                    }
                )
            )
            # 执行语句
            session.execute(stmt)

        # 提交事务
        session.commit()
    except SQLAlchemyError as e:
        # 如果发生错误，回滚事务
        session.rollback()
        print(f"An error occurred: {e}")
    finally:
        # 关闭会话
        session.close()

if __name__ == "__main__":
    init_log("em_yjyg.log")

    record_stock_data()

    sched.start()

    sched._thread.join()