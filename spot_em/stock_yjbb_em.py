# 将业绩快报保存到数据库
#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
Date: 2022/11/12 21:27
Desc: 东方财富-数据中心-年报季报
东方财富-数据中心-年报季报-业绩快报-业绩报表
https://data.eastmoney.com/bbsj/202003/yjbb.html
"""
import pandas as pd
import requests
from tqdm import tqdm
from spot_em.utils.tqdm import get_tqdm

def stock_yjbb_em(date: str = "20200331") -> pd.DataFrame:
    """
    东方财富-数据中心-年报季报-业绩快报-业绩报表
    https://data.eastmoney.com/bbsj/202003/yjbb.html
    :param date: "20200331", "20200630", "20200930", "20201231"; 从 20100331 开始
    :type date: str
    :return: 业绩报表
    :rtype: pandas.DataFrame
    """
    import warnings

    warnings.simplefilter(action="ignore", category=FutureWarning)  # 忽略所有
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
    params = {
        "sortColumns": "UPDATE_DATE,SECURITY_CODE",
        "sortTypes": "-1,-1",
        "pageSize": "500",
        "pageNumber": "1",
        "reportName": "RPT_LICO_FN_CPD",
        "columns": "ALL",
        "filter": f"(REPORTDATE='{'-'.join([date[:4], date[4:6], date[6:]])}')",
    }
    r = requests.get(url, params=params)
    data_json = r.json()
    page_num = data_json["result"]["pages"]
    big_df = pd.DataFrame()
    tqdm = get_tqdm()
    big_list = []
    for page in tqdm(range(1, page_num + 1), leave=False):
        params.update(
            {
                "pageNumber": page,
            }
        )
        r = requests.get(url, params=params)
        data_json = r.json()
        temp_df = pd.DataFrame(data_json["result"]["data"])
        big_list.append(temp_df)
    big_df = pd.concat(big_list, ignore_index=True)
    big_df.reset_index(inplace=True)
    big_df["index"] = range(1, len(big_df) + 1)
    big_df.columns = [
        "序号",
        "股票代码",
        "股票简称",
        "_",
        "_",
        "_",
        "_",
        "最新公告日期",
        "_",
        "每股收益",
        "_",
        "营业总收入-营业总收入",
        "净利润-净利润",
        "净资产收益率",
        "营业总收入-同比增长",
        "净利润-同比增长",
        "每股净资产",
        "每股经营现金流量",
        "销售毛利率",
        "营业总收入-季度环比增长",
        "净利润-季度环比增长",
        "_",
        "_",
        "所处行业",
        "_",
        "_",
        "_",
        "_",
        "_",
        "_",
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
            "营业总收入-营业总收入",
            "营业总收入-同比增长",
            "营业总收入-季度环比增长",
            "净利润-净利润",
            "净利润-同比增长",
            "净利润-季度环比增长",
            "每股净资产",
            "净资产收益率",
            "每股经营现金流量",
            "销售毛利率",
            "所处行业",
            "最新公告日期",
        ]
    ]
    big_df["每股收益"] = pd.to_numeric(big_df["每股收益"], errors="coerce")
    big_df["营业总收入-营业总收入"] = pd.to_numeric(
        big_df["营业总收入-营业总收入"], errors="coerce"
    )
    big_df["营业总收入-同比增长"] = pd.to_numeric(
        big_df["营业总收入-同比增长"], errors="coerce"
    )
    big_df["营业总收入-季度环比增长"] = pd.to_numeric(
        big_df["营业总收入-季度环比增长"], errors="coerce"
    )
    big_df["净利润-净利润"] = pd.to_numeric(big_df["净利润-净利润"], errors="coerce")
    big_df["净利润-同比增长"] = pd.to_numeric(
        big_df["净利润-同比增长"], errors="coerce"
    )
    big_df["净利润-季度环比增长"] = pd.to_numeric(
        big_df["净利润-季度环比增长"], errors="coerce"
    )
    big_df["每股净资产"] = pd.to_numeric(big_df["每股净资产"], errors="coerce")
    big_df["净资产收益率"] = pd.to_numeric(big_df["净资产收益率"], errors="coerce")
    big_df["每股经营现金流量"] = pd.to_numeric(
        big_df["每股经营现金流量"], errors="coerce"
    )
    big_df["销售毛利率"] = pd.to_numeric(big_df["销售毛利率"], errors="coerce")
    big_df["最新公告日期"] = pd.to_datetime(
        big_df["最新公告日期"], errors="coerce"
    ).dt.date
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
    month = (now.month - 1) - (now.month - 1) % 3 + 1
    this_quarter_start = datetime.datetime(now.year , month, 1)
    # 上季第一天和最后一天
    last_quarter_end = this_quarter_start - timedelta(days=1)
    last_quarter_end = last_quarter_end.strftime("%Y%m%d")
    stock_yjbb_em_df = stock_yjbb_em(date=last_quarter_end)
    print(stock_yjbb_em_df)
    if len(stock_yjbb_em_df) == 0:
        logger.info("上季度还没有数据")
        return False
    stock_yjbb_em_df['最新公告日期'] = pd.to_datetime(stock_yjbb_em_df['最新公告日期'])
    stock_yjbb_em_df['最新公告日期'] = stock_yjbb_em_df['最新公告日期'].dt.strftime('%Y-%m-%d')
    stock_yjbb_em_df['序号'] = stock_yjbb_em_df['股票代码'] + '_' + stock_yjbb_em_df['最新公告日期']

    stock_yjbb_em_df['营业收入-营业收入'] = stock_yjbb_em_df['营业总收入-营业总收入']
    stock_yjbb_em_df['营业收入-同比增长'] = stock_yjbb_em_df['营业总收入-同比增长']
    stock_yjbb_em_df['营业收入-季度环比增长'] = stock_yjbb_em_df['营业总收入-季度环比增长']
    # Assuming 'df' is your DataFrame and it has '营业收入_季度环比增长' and '净利润_季度环比增长' columns
    stock_yjbb_em_df = stock_yjbb_em_df.fillna(
        value=np.nan)  # Make sure all NaNs are explicitly set if they weren't already

    # Replace NaN with a default value, e.g., 0
    stock_yjbb_em_df.replace({np.nan: 0}, inplace=True)

    from sqlalchemy import create_engine, Column, Integer, String, Float, Date, insert
    from sqlalchemy.orm import sessionmaker, declarative_base
    from zvtm import zvt_config
    DATABASE_URI = 'mysql+pymysql://' + zvt_config['mysql_user'] + ':' + zvt_config['mysql_password'] + '@' + \
                   zvt_config['mysql_host'] + ':' + zvt_config['mysql_port'] + '/eastmoney_finance'

    # 创建数据库引擎
    engine = create_engine(DATABASE_URI, echo=False)

    # 定义模型基类
    Base = declarative_base()

    # 定义数据库表模型
    class StockInfo(Base):
        __tablename__ = 'em_yjbb'

        序号 = Column(String(length=128), primary_key=True)
        股票代码 = Column(String(20))
        股票简称 = Column(String(50))
        每股收益 = Column(Float)
        营业收入_营业收入 = Column(Float)
        营业收入_同比增长 = Column(Float)
        营业收入_季度环比增长 = Column(Float)
        净利润_净利润 = Column(Float)
        净利润_同比增长 = Column(Float)
        净利润_季度环比增长 = Column(Float)
        每股净资产 = Column(Float)
        净资产收益率 = Column(Float)
        所处行业 = Column(String(100))
        最新公告日期 = Column(Date)

        def __repr__(self):
            return f"<StockInfo(序号={self.序号}, 股票代码='{self.股票代码}', 股票简称='{self.股票简称}')>"

    # 创建表（如果表不存在）
    Base.metadata.create_all(engine)

    # 创建 Session 类型
    Session = sessionmaker(bind=engine)

    # 创建 Session 实例
    session = Session()
    insert_stmt = None
    # 将 DataFrame 转换为 SQLAlchemy 模型的列表
    for index, row in stock_yjbb_em_df.iterrows():
        # 创建 StockInfo 实例
        stmt = insert(StockInfo).values(
            序号=row['序号'],
            股票代码=row['股票代码'],
            股票简称=row['股票简称'],
            每股收益=row['每股收益'],
            营业收入_营业收入=row['营业收入-营业收入'],
            营业收入_同比增长=row['营业收入-同比增长'],
            营业收入_季度环比增长=row['营业收入-季度环比增长'],
            净利润_净利润=row['净利润-净利润'],
            净利润_同比增长=row['净利润-同比增长'],
            净利润_季度环比增长=row['净利润-季度环比增长'],
            每股净资产=row['每股净资产'],
            净资产收益率=row['净资产收益率'],
            所处行业=row['所处行业'],
            最新公告日期=row['最新公告日期']
        ).prefix_with("IGNORE")

        # 将语句添加到会话
        session.execute(stmt)

    # 提交事务
    session.commit()

    # 关闭 Session
    session.close()

if __name__ == "__main__":

    init_log("em_yjbb.log")

    record_stock_data()

    # sched.start()
    #
    # sched._thread.join()
