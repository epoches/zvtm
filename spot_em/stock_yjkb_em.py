#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
Date: 2022/11/25 12:10
Desc: 东方财富-数据中心-年报季报
东方财富-数据中心-年报季报-业绩预告
https://data.eastmoney.com/bbsj/202003/yjyg.html
东方财富-数据中心-年报季报-预约披露时间
https://data.eastmoney.com/bbsj/202003/yysj.html
"""
import pandas as pd
import requests
from tqdm import tqdm


def stock_yjkb_em(date: str = "20211231") -> pd.DataFrame:
    """
    东方财富-数据中心-年报季报-业绩快报
    https://data.eastmoney.com/bbsj/202003/yjkb.html
    :param date: "20200331", "20200630", "20200930", "20201231"; 从 20100331 开始
    :type date: str
    :return: 业绩快报
    :rtype: pandas.DataFrame
    """
    url = "https://datacenter.eastmoney.com/securities/api/data/v1/get"
    params = {
        "sortColumns": "UPDATE_DATE,SECURITY_CODE",
        "sortTypes": "-1,-1",
        "pageSize": "500",
        "pageNumber": "1",
        "reportName": "RPT_FCI_PERFORMANCEE",
        "columns": "ALL",
        "filter": f"""(SECURITY_TYPE_CODE in ("058001001","058001008"))(TRADE_MARKET_CODE!="069001017")(REPORT_DATE='{'-'.join([date[:4], date[4:6], date[6:]])}')""",
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
        "股票代码",
        "股票简称",
        "市场板块",
        "_",
        "证券类型",
        "_",
        "公告日期",
        "_",
        "每股收益",
        "营业收入-营业收入",
        "营业收入-去年同期",
        "净利润-净利润",
        "净利润-去年同期",
        "每股净资产",
        "净资产收益率",
        "营业收入-同比增长",
        "净利润-同比增长",
        "营业收入-季度环比增长",
        "净利润-季度环比增长",
        "所处行业",
        "_",
        "_",
        "_",
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
            "每股收益",
            "营业收入-营业收入",
            "营业收入-去年同期",
            "营业收入-同比增长",
            "营业收入-季度环比增长",
            "净利润-净利润",
            "净利润-去年同期",
            "净利润-同比增长",
            "净利润-季度环比增长",
            "每股净资产",
            "净资产收益率",
            "所处行业",
            "公告日期",
        ]
    ]
    big_df["每股收益"] = pd.to_numeric(big_df["每股收益"], errors="coerce")
    big_df["营业收入-营业收入"] = pd.to_numeric(big_df["营业收入-营业收入"], errors="coerce")
    big_df["营业收入-去年同期"] = pd.to_numeric(big_df["营业收入-去年同期"], errors="coerce")
    big_df["营业收入-同比增长"] = pd.to_numeric(big_df["营业收入-同比增长"], errors="coerce")
    big_df["营业收入-季度环比增长"] = pd.to_numeric(big_df["营业收入-季度环比增长"], errors="coerce")
    big_df["净利润-净利润"] = pd.to_numeric(big_df["净利润-净利润"], errors="coerce")
    big_df["净利润-去年同期"] = pd.to_numeric(big_df["净利润-去年同期"], errors="coerce")
    big_df["净利润-同比增长"] = pd.to_numeric(big_df["净利润-同比增长"], errors="coerce")
    big_df["净利润-季度环比增长"] = pd.to_numeric(big_df["净利润-季度环比增长"], errors="coerce")
    big_df["每股净资产"] = pd.to_numeric(big_df["每股净资产"], errors="coerce")
    big_df["净资产收益率"] = pd.to_numeric(big_df["净资产收益率"], errors="coerce")
    big_df["净资产收益率"] = pd.to_numeric(big_df["净资产收益率"], errors="coerce")
    big_df["公告日期"] = pd.to_datetime(big_df["公告日期"]).dt.date
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
@sched.scheduled_job('cron',day_of_week='mon-fri', hour=20, minute=00)
def record_stock_data():
    now = datetime.datetime.now()
    now = now.strftime("%Y%m%d")
    stock_yjkb_em_df = stock_yjkb_em(date=now)
    print(stock_yjkb_em_df)
    if len(stock_yjkb_em_df) == 0:
        logger.info("今日没有数据")
        return False
    from sqlalchemy import create_engine, MetaData, Table, Column, String, Float, BLOB, Date
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy.dialects.mysql import insert
    from sqlalchemy.exc import SQLAlchemyError
    from zvtm import zvt_config
    DATABASE_URI = 'mysql+pymysql://' + zvt_config['mysql_user'] + ':' + zvt_config['mysql_password'] + '@' + \
                   zvt_config['mysql_host'] + ':' + zvt_config['mysql_port'] + '/eastmoney_finance'

    # 创建数据库引擎
    engine = create_engine(DATABASE_URI, echo=True)
    # 创建会话类
    Session = sessionmaker(bind=engine)
    # 创建会话
    session = Session()

    # 假设 stock_ykb_em_df 已经是一个 pandas DataFrame
    # 将日期列转换为 datetime 并格式化
    stock_yjkb_em_df['公告日期'] = pd.to_datetime(stock_yjkb_em_df['公告日期'])
    stock_yjkb_em_df['公告日期'] = stock_yjkb_em_df['公告日期'].dt.strftime('%Y-%m-%d')

    # 为每条记录创建一个唯一的序号
    stock_yjkb_em_df['序号'] = stock_yjkb_em_df['股票代码'] + '_' + stock_yjkb_em_df['公告日期']

    # 填充 NaN 值
    stock_yjkb_em_df = stock_yjkb_em_df.fillna(
        value=np.nan)  # 确保所有 NaNs 都被明确设置
    # 将 NaN 替换为默认值，例如 0
    stock_yjkb_em_df.replace({np.nan: 0}, inplace=True)

    # 定义数据库元数据
    metadata = MetaData()

    # 定义表结构，这里需要根据实际的数据库表结构进行修改
    em_yjkb = Table(
        'em_yjkb', metadata,
        Column('序号', String(128), primary_key=True),
        Column('股票代码', String(20)),
        Column('股票简称', String(50)),
        Column('每股收益', Float),
        Column('营业收入-营业收入', Float),
        Column('营业收入-去年同期', Float),
        Column('营业收入-同比增长', Float),
        Column('营业收入-季度环比增长', Float),
        Column('净利润-净利润', Float),
        Column('净利润-去年同期', Float),
        Column('净利润-同比增长', Float),
        Column('净利润-季度环比增长', Float),
        Column('每股净资产', Float),
        Column('净资产收益率', Float),
        Column('所处行业', String(100)),
        Column('公告日期', Date)
    )

    # 如果表不存在则创建表
    metadata.create_all(engine)

    try:
        for index, row in stock_yjkb_em_df.iterrows():
            # 创建一个 insert() 语句，如果遇到主键冲突则更新记录
            stmt = (
                insert(em_yjkb).values({
                    '序号': row['序号'],
                    '股票代码': row['股票代码'],
                    '股票简称': row['股票简称'],
                    '每股收益': row['每股收益'],
                    '营业收入-营业收入': row['营业收入-营业收入'],
                    '营业收入-去年同期': row['营业收入-去年同期'],
                    '营业收入-同比增长': row['营业收入-同比增长'],
                    '营业收入-季度环比增长': row['营业收入-季度环比增长'],
                    '净利润-净利润': row['净利润-净利润'],
                    '净利润-去年同期': row['净利润-去年同期'],
                    '净利润-同比增长': row['净利润-同比增长'],
                    '净利润-季度环比增长': row['净利润-季度环比增长'],
                    '每股净资产': row['每股净资产'],
                    '净资产收益率': row['净资产收益率'],
                    '所处行业': row['所处行业'],
                    '公告日期': row['公告日期']
                })
                    .on_duplicate_key_update({  # 使用字典指定更新的列
                    '股票代码': row['股票代码'],
                    '股票简称': row['股票简称'],
                    # ... 其他列
                    # '公告日期': row['公告日期']
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

    init_log("em_yjkb.log")

    record_stock_data()

    sched.start()

    sched._thread.join()