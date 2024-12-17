#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
Date: 2024/2/9 11:40
Desc: 东方财富网-数据中心-特色数据-股权质押
东方财富网-数据中心-特色数据-股权质押-股权质押市场概况: https://data.eastmoney.com/gpzy/marketProfile.aspx
东方财富网-数据中心-特色数据-股权质押-上市公司质押比例: https://data.eastmoney.com/gpzy/pledgeRatio.aspx
东方财富网-数据中心-特色数据-股权质押-重要股东股权质押明细: https://data.eastmoney.com/gpzy/pledgeDetail.aspx
东方财富网-数据中心-特色数据-股权质押-质押机构分布统计-证券公司: https://data.eastmoney.com/gpzy/distributeStatistics.aspx
东方财富网-数据中心-特色数据-股权质押-质押机构分布统计-银行: https://data.eastmoney.com/gpzy/distributeStatistics.aspx
东方财富网-数据中心-特色数据-股权质押-行业数据: https://data.eastmoney.com/gpzy/industryData.aspx
"""
import math

import pandas as pd
import requests
from tqdm import tqdm


def stock_gpzy_profile_em() -> pd.DataFrame:
    """
    东方财富网-数据中心-特色数据-股权质押-股权质押市场概况
    https://data.eastmoney.com/gpzy/marketProfile.aspx
    :return: 股权质押市场概况
    :rtype: pandas.DataFrame
    """
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
    params = {
        "sortColumns": "TRADE_DATE",
        "sortTypes": "-1",
        "pageSize": "5000",
        "pageNumber": "1",
        "reportName": "RPT_CSDC_STATISTICS",
        "columns": "ALL",
        "quoteColumns": "",
        "source": "WEB",
        "client": "WEB",
    }
    r = requests.get(url, params=params)
    data_json = r.json()
    temp_df = pd.DataFrame(data_json["result"]["data"])
    temp_df.columns = [
        "交易日期",
        "质押总股数",
        "质押总市值",
        "沪深300指数",
        "涨跌幅",
        "A股质押总比例",
        "质押公司数量",
        "质押笔数",
    ]
    temp_df = temp_df[
        [
            "交易日期",
            "A股质押总比例",
            "质押公司数量",
            "质押笔数",
            "质押总股数",
            "质押总市值",
            "沪深300指数",
            "涨跌幅",
        ]
    ]
    temp_df["交易日期"] = pd.to_datetime(temp_df["交易日期"]).dt.date
    temp_df["A股质押总比例"] = pd.to_numeric(temp_df["A股质押总比例"])
    temp_df["质押公司数量"] = pd.to_numeric(temp_df["质押公司数量"])
    temp_df["质押笔数"] = pd.to_numeric(temp_df["质押笔数"])
    temp_df["质押总股数"] = pd.to_numeric(temp_df["质押总股数"])
    temp_df["质押总市值"] = pd.to_numeric(temp_df["质押总市值"])
    temp_df["沪深300指数"] = pd.to_numeric(temp_df["沪深300指数"])
    temp_df["涨跌幅"] = pd.to_numeric(temp_df["涨跌幅"])

    temp_df["A股质押总比例"] = temp_df["A股质押总比例"] / 100

    temp_df.sort_values(["交易日期"], inplace=True)
    temp_df.reset_index(inplace=True, drop=True)
    return temp_df


def stock_gpzy_pledge_ratio_em(date: str = "20231020") -> pd.DataFrame:
    """
    东方财富网-数据中心-特色数据-股权质押-上市公司质押比例
    https://data.eastmoney.com/gpzy/pledgeRatio.aspx
    :param date: 指定交易日, 访问 https://data.eastmoney.com/gpzy/pledgeRatio.aspx 查询
    :type date: str
    :return: 上市公司质押比例
    :rtype: pandas.DataFrame
    """
    trade_date = "-".join([date[:4], date[4:6], date[6:]])
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
    params = {
        "sortColumns": "PLEDGE_RATIO",
        "sortTypes": "-1",
        "pageSize": "500",
        "pageNumber": "1",
        "reportName": "RPT_CSDC_LIST",
        "columns": "ALL",
        "quoteColumns": "",
        "source": "WEB",
        "client": "WEB",
        "filter": f"(TRADE_DATE='{trade_date}')",
    }
    r = requests.get(url, params=params)
    data_json = r.json()
    if data_json['code'] == 9201:
        print('当日无数据')
        return
    total_page = data_json["result"]["pages"]
    big_df = pd.DataFrame()
    for page in tqdm(range(1, total_page + 1), leave=False):
        params.update({"pageNumber": page})
        r = requests.get(url, params=params)
        data_json = r.json()
        temp_df = pd.DataFrame(data_json["result"]["data"])
        big_df = pd.concat([big_df, temp_df], ignore_index=True)

    big_df.reset_index(inplace=True)
    big_df["index"] = big_df.index + 1
    big_df.columns = [
        "序号",
        "-",
        "股票代码",
        "股票简称",
        "交易日期",
        "质押比例",
        "质押股数",
        "质押笔数",
        "无限售股质押数",
        "限售股质押数",
        "质押市值",
        "所属行业",
        "近一年涨跌幅",
        "所属行业代码",
        "-",
    ]
    big_df = big_df[
        [
            "序号",
            "股票代码",
            "股票简称",
            "交易日期",
            "所属行业",
            "质押比例",
            "质押股数",
            "质押市值",
            "质押笔数",
            "无限售股质押数",
            "限售股质押数",
            "近一年涨跌幅",
            "所属行业代码",
        ]
    ]
    big_df["质押比例"] = pd.to_numeric(big_df["质押比例"], errors="coerce")
    big_df["质押股数"] = pd.to_numeric(big_df["质押股数"], errors="coerce")
    big_df["质押市值"] = pd.to_numeric(big_df["质押市值"], errors="coerce")
    big_df["质押笔数"] = pd.to_numeric(big_df["质押笔数"], errors="coerce")
    big_df["无限售股质押数"] = pd.to_numeric(big_df["无限售股质押数"], errors="coerce")
    big_df["限售股质押数"] = pd.to_numeric(big_df["限售股质押数"], errors="coerce")
    big_df["近一年涨跌幅"] = pd.to_numeric(big_df["近一年涨跌幅"], errors="coerce")
    big_df["交易日期"] = pd.to_datetime(big_df["交易日期"], errors="coerce").dt.date
    return big_df


def _get_page_num_gpzy_market_pledge_ratio_detail() -> int:
    """
    东方财富网-数据中心-特色数据-股权质押-重要股东股权质押明细
    https://data.eastmoney.com/gpzy/pledgeDetail.aspx
    :return: int 获取 重要股东股权质押明细 的总页数
    """
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
    params = {
        "sortColumns": "NOTICE_DATE",
        "sortTypes": "-1",
        "pageSize": "500",
        "pageNumber": "1",
        "reportName": "RPTA_APP_ACCUMDETAILS",
        "columns": "ALL",
        "quoteColumns": "",
        "source": "WEB",
        "client": "WEB",
    }
    r = requests.get(url, params=params)
    data_json = r.json()
    total_page = math.ceil(int(data_json["result"]["count"]) / 500)
    return total_page


def stock_gpzy_pledge_ratio_detail_em() -> pd.DataFrame:
    """
    东方财富网-数据中心-特色数据-股权质押-重要股东股权质押明细
    https://data.eastmoney.com/gpzy/pledgeDetail.aspx
    :return: pandas.DataFrame
    """
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
    total_page = _get_page_num_gpzy_market_pledge_ratio_detail()
    big_df = pd.DataFrame()
    for page in tqdm(range(1, total_page + 1), leave=False):
        params = {
            "sortColumns": "NOTICE_DATE",
            "sortTypes": "-1",
            "pageSize": "500",
            "pageNumber": page,
            "reportName": "RPTA_APP_ACCUMDETAILS",
            "columns": "ALL",
            "quoteColumns": "",
            "source": "WEB",
            "client": "WEB",
        }
        r = requests.get(url, params=params)
        data_json = r.json()
        temp_df = pd.DataFrame(data_json["result"]["data"])
        big_df = pd.concat(objs=[big_df, temp_df], ignore_index=True)
    big_df.reset_index(inplace=True)
    big_df["index"] = big_df.index + 1
    big_df.columns = [
        "序号",
        "股票简称",
        "_",
        "股票代码",
        "股东名称",
        "_",
        "_",
        "_",
        "公告日期",
        "质押机构",
        "质押股份数量",
        "占所持股份比例",
        "占总股本比例",
        "质押日收盘价",
        "质押开始日期",
        "_",
        "_",
        "_",
        "_",
        "_",
        "_",
        "_",
        "预估平仓线",
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
        "最新价",
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
            "股东名称",
            "质押股份数量",
            "占所持股份比例",
            "占总股本比例",
            "质押机构",
            "最新价",
            "质押日收盘价",
            "预估平仓线",
            "质押开始日期",
            "公告日期",
        ]
    ]

    big_df["质押股份数量"] = pd.to_numeric(big_df["质押股份数量"], errors="coerce")
    big_df["占所持股份比例"] = pd.to_numeric(big_df["占所持股份比例"], errors="coerce")
    big_df["占总股本比例"] = pd.to_numeric(big_df["占总股本比例"], errors="coerce")
    big_df["最新价"] = pd.to_numeric(big_df["最新价"], errors="coerce")
    big_df["质押日收盘价"] = pd.to_numeric(big_df["质押日收盘价"], errors="coerce")
    big_df["预估平仓线"] = pd.to_numeric(big_df["预估平仓线"], errors="coerce")
    big_df["公告日期"] = pd.to_datetime(big_df["公告日期"], errors="coerce").dt.date
    big_df["质押开始日期"] = pd.to_datetime(big_df["质押开始日期"], errors="coerce").dt.date
    return big_df

def stock_gpzy_pledge_ratio_detail_page1_em() -> pd.DataFrame:
    """
    东方财富网-数据中心-特色数据-股权质押-重要股东股权质押明细
    https://data.eastmoney.com/gpzy/pledgeDetail.aspx
    :return: pandas.DataFrame
    """
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"

    big_df = pd.DataFrame()

    params = {
        "sortColumns": "NOTICE_DATE",
        "sortTypes": "-1",
        "pageSize": "500",
        "pageNumber": 1,
        "reportName": "RPTA_APP_ACCUMDETAILS",
        "columns": "ALL",
        "quoteColumns": "",
        "source": "WEB",
        "client": "WEB",
    }
    r = requests.get(url, params=params)
    data_json = r.json()
    temp_df = pd.DataFrame(data_json["result"]["data"])
    big_df = pd.concat(objs=[big_df, temp_df], ignore_index=True)
    big_df.reset_index(inplace=True)
    big_df["index"] = big_df.index + 1
    big_df.columns = [
        "序号",
        "股票简称",
        "_",
        "股票代码",
        "股东名称",
        "_",
        "_",
        "_",
        "公告日期",
        "质押机构",
        "质押股份数量",
        "占所持股份比例",
        "占总股本比例",
        "质押日收盘价",
        "质押开始日期",
        "_",
        "_",
        "_",
        "_",
        "_",
        "_",
        "_",
        "预估平仓线",
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
        "最新价",
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
            "股东名称",
            "质押股份数量",
            "占所持股份比例",
            "占总股本比例",
            "质押机构",
            "最新价",
            "质押日收盘价",
            "预估平仓线",
            "质押开始日期",
            "公告日期",
        ]
    ]

    big_df["质押股份数量"] = pd.to_numeric(big_df["质押股份数量"], errors="coerce")
    big_df["占所持股份比例"] = pd.to_numeric(big_df["占所持股份比例"], errors="coerce")
    big_df["占总股本比例"] = pd.to_numeric(big_df["占总股本比例"], errors="coerce")
    big_df["最新价"] = pd.to_numeric(big_df["最新价"], errors="coerce")
    big_df["质押日收盘价"] = pd.to_numeric(big_df["质押日收盘价"], errors="coerce")
    big_df["预估平仓线"] = pd.to_numeric(big_df["预估平仓线"], errors="coerce")
    big_df["公告日期"] = pd.to_datetime(big_df["公告日期"], errors="coerce").dt.date
    big_df["质押开始日期"] = pd.to_datetime(big_df["质押开始日期"], errors="coerce").dt.date
    return big_df

def stock_gpzy_distribute_statistics_company_em() -> pd.DataFrame:
    """
    东方财富网-数据中心-特色数据-股权质押-质押机构分布统计-证券公司
    https://data.eastmoney.com/gpzy/distributeStatistics.aspx
    :return: 质押机构分布统计-证券公司
    :rtype: pandas.DataFrame
    """
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
    params = {
        "sortColumns": "ORG_NUM",
        "sortTypes": "-1",
        "pageSize": "500",
        "pageNumber": "1",
        "reportName": "RPT_GDZY_ZYJG_SUM",
        "columns": "ALL",
        "quoteColumns": "",
        "source": "WEB",
        "client": "WEB",
        "filter": '(PFORG_TYPE="证券")',
    }
    r = requests.get(url, params=params)
    data_json = r.json()
    temp_df = pd.DataFrame(data_json["result"]["data"])
    temp_df.reset_index(inplace=True)
    temp_df["index"] = temp_df.index + 1
    temp_df.columns = [
        "序号",
        "_",
        "_",
        "_",
        "_",
        "质押机构",
        "_",
        "质押公司数量",
        "质押笔数",
        "质押数量",
        "未达预警线比例",
        "达到预警线未达平仓线比例",
        "达到平仓线比例",
        "_",
        "_",
    ]
    temp_df = temp_df[
        [
            "序号",
            "质押机构",
            "质押公司数量",
            "质押笔数",
            "质押数量",
            "未达预警线比例",
            "达到预警线未达平仓线比例",
            "达到平仓线比例",
        ]
    ]
    temp_df["质押公司数量"] = pd.to_numeric(temp_df["质押公司数量"])
    temp_df["质押笔数"] = pd.to_numeric(temp_df["质押笔数"])
    temp_df["质押数量"] = pd.to_numeric(temp_df["质押数量"])
    temp_df["未达预警线比例"] = pd.to_numeric(temp_df["未达预警线比例"])
    temp_df["达到预警线未达平仓线比例"] = pd.to_numeric(temp_df["达到预警线未达平仓线比例"])
    temp_df["达到平仓线比例"] = pd.to_numeric(temp_df["达到平仓线比例"])
    return temp_df


def stock_gpzy_distribute_statistics_bank_em() -> pd.DataFrame:
    """
    东方财富网-数据中心-特色数据-股权质押-质押机构分布统计-银行
    https://data.eastmoney.com/gpzy/distributeStatistics.aspx
    :return: 质押机构分布统计-银行
    :rtype: pandas.DataFrame
    """
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
    params = {
        "sortColumns": "ORG_NUM",
        "sortTypes": "-1",
        "pageSize": "500",
        "pageNumber": "1",
        "reportName": "RPT_GDZY_ZYJG_SUM",
        "columns": "ALL",
        "quoteColumns": "",
        "source": "WEB",
        "client": "WEB",
        "filter": '(PFORG_TYPE="银行")',
    }
    r = requests.get(url, params=params)
    data_json = r.json()
    temp_df = pd.DataFrame(data_json["result"]["data"])
    temp_df.reset_index(inplace=True)
    temp_df["index"] = temp_df.index + 1
    temp_df.columns = [
        "序号",
        "_",
        "_",
        "_",
        "_",
        "质押机构",
        "_",
        "质押公司数量",
        "质押笔数",
        "质押数量",
        "未达预警线比例",
        "达到预警线未达平仓线比例",
        "达到平仓线比例",
        "_",
        "_",
    ]
    temp_df = temp_df[
        [
            "序号",
            "质押机构",
            "质押公司数量",
            "质押笔数",
            "质押数量",
            "未达预警线比例",
            "达到预警线未达平仓线比例",
            "达到平仓线比例",
        ]
    ]
    temp_df["质押公司数量"] = pd.to_numeric(temp_df["质押公司数量"])
    temp_df["质押笔数"] = pd.to_numeric(temp_df["质押笔数"])
    temp_df["质押数量"] = pd.to_numeric(temp_df["质押数量"])
    temp_df["未达预警线比例"] = pd.to_numeric(temp_df["未达预警线比例"])
    temp_df["达到预警线未达平仓线比例"] = pd.to_numeric(temp_df["达到预警线未达平仓线比例"])
    temp_df["达到平仓线比例"] = pd.to_numeric(temp_df["达到平仓线比例"])
    return temp_df


def stock_gpzy_industry_data_em() -> pd.DataFrame:
    """
    东方财富网-数据中心-特色数据-股权质押-上市公司质押比例-行业数据
    https://data.eastmoney.com/gpzy/industryData.aspx
    :return: pandas.DataFrame
    """
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
    params = {
        "sortColumns": "AVERAGE_PLEDGE_RATIO",
        "sortTypes": "-1",
        "pageSize": "500",
        "pageNumber": "1",
        "reportName": "RPT_CSDC_INDUSTRY_STATISTICS",
        "columns": "INDUSTRY_CODE,INDUSTRY,TRADE_DATE,AVERAGE_PLEDGE_RATIO,ORG_NUM,PLEDGE_TOTAL_NUM,TOTAL_PLEDGE_SHARES,PLEDGE_TOTAL_MARKETCAP",
        "quoteColumns": "",
        "source": "WEB",
        "client": "WEB",
    }
    r = requests.get(url, params=params)
    data_json = r.json()
    temp_df = pd.DataFrame(data_json["result"]["data"])
    temp_df.reset_index(inplace=True)
    temp_df["index"] = temp_df.index + 1
    temp_df.columns = [
        "序号",
        "-",
        "行业",
        "统计时间",
        "平均质押比例",
        "公司家数",
        "质押总笔数",
        "质押总股本",
        "最新质押市值",
    ]
    temp_df = temp_df[
        ["序号", "行业", "平均质押比例", "公司家数", "质押总笔数", "质押总股本", "最新质押市值", "统计时间"]
    ]
    temp_df["统计时间"] = pd.to_datetime(temp_df["统计时间"]).dt.date

    temp_df["平均质押比例"] = pd.to_numeric(temp_df["平均质押比例"])
    temp_df["公司家数"] = pd.to_numeric(temp_df["公司家数"])
    temp_df["质押总笔数"] = pd.to_numeric(temp_df["质押总笔数"])
    temp_df["质押总股本"] = pd.to_numeric(temp_df["质押总股本"])
    temp_df["最新质押市值"] = pd.to_numeric(temp_df["最新质押市值"])

    return temp_df



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
    stock_gpzy_pledge_ratio_detail_em_df = stock_gpzy_pledge_ratio_detail_page1_em()
    # print(stock_gpzy_pledge_ratio_detail_em_df)
    # stock_gpzy_pledge_ratio_detail_em_df = stock_gpzy_pledge_ratio_detail_em()
    # print(stock_gpzy_pledge_ratio_detail_em_df)
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
    stock_gpzy_pledge_ratio_detail_em_df['公告日期'] = pd.to_datetime(stock_gpzy_pledge_ratio_detail_em_df['公告日期'])
    stock_gpzy_pledge_ratio_detail_em_df['公告日期'] = stock_gpzy_pledge_ratio_detail_em_df['公告日期'].dt.strftime(
        '%Y-%m-%d')

    # 为每条记录创建一个唯一的序号
    stock_gpzy_pledge_ratio_detail_em_df['序号'] = stock_gpzy_pledge_ratio_detail_em_df['股票代码'] + '_' + \
                                                 stock_gpzy_pledge_ratio_detail_em_df['公告日期']

    # 填充 NaN 值
    stock_gpzy_pledge_ratio_detail_em_df = stock_gpzy_pledge_ratio_detail_em_df.fillna(
        value=np.nan)  # 确保所有 NaNs 都被明确设置
    # 将 NaN 替换为默认值，例如 0
    stock_gpzy_pledge_ratio_detail_em_df.replace({np.nan: 0}, inplace=True)
    stock_gpzy_pledge_ratio_detail_em_df['质押开始日期'].replace({0: '1970-01-01'}, inplace=True)
    stock_gpzy_pledge_ratio_detail_em_df['质押开始日期'] = pd.to_datetime(stock_gpzy_pledge_ratio_detail_em_df['质押开始日期'])
    stock_gpzy_pledge_ratio_detail_em_df['质押开始日期'] = stock_gpzy_pledge_ratio_detail_em_df['质押开始日期'].dt.strftime(
        '%Y-%m-%d')
    metadata = MetaData()

    # 定义新的表结构
    stock_pledge_management = Table(
        'stock_pledge_management', metadata,
        Column('序号', String(128), primary_key=True),
        Column('股票代码', String(20)),
        Column('股票简称', String(50)),
        Column('股东名称', String(50)),
        Column('质押股份数量', Float),
        Column('占所持股份比例', Float),
        Column('占总股本比例', Float),
        Column('质押机构', String(250)),
        Column('最新价', Float),
        Column('质押日收盘价', Float),
        Column('预估平仓线', Float),
        Column('质押开始日期', Date),
        Column('公告日期', Date)
    )

    # 如果表不存在则创建表
    metadata.create_all(engine)

    # 假设stock_gpzy_pledge_ratio_detail_em_df是您提供的DataFrame
    # 以下是将DataFrame保存到数据库的代码
    try:
        # 遍历DataFrame中的每一行
        for index, row in stock_gpzy_pledge_ratio_detail_em_df.iterrows():
            # 创建一个insert()语句，如果遇到主键冲突则更新记录
            stmt = (
                insert(stock_pledge_management).values({
                    '序号': row['序号'],
                    '股票代码': row['股票代码'],
                    '股票简称': row['股票简称'],
                    '股东名称': row['股东名称'],
                    '质押股份数量': row['质押股份数量'],
                    '占所持股份比例': row['占所持股份比例'],
                    '占总股本比例': row['占总股本比例'],
                    '质押机构': row['质押机构'],
                    '最新价': row['最新价'],
                    '质押日收盘价': row['质押日收盘价'],
                    '预估平仓线': row['预估平仓线'],
                    '质押开始日期': row['质押开始日期'],
                    '公告日期': row['公告日期']
                })
                    .on_duplicate_key_update(  # 使用字典指定更新的列
                    {'股票代码': row['股票代码'],
                     '股票简称': row['股票简称'],
                     '股东名称': row['股东名称'],
                     # ... 其他列
                     '公告日期': row['公告日期']
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
    # stock_gpzy_profile_em_df = stock_gpzy_profile_em()
    # print(stock_gpzy_profile_em_df)
    #['序号', '股票代码', '股票简称', '交易日期', '所属行业', '质押比例', '质押股数', '质押市值', '质押笔数',
      # '无限售股质押数', '限售股质押数', '近一年涨跌幅', '所属行业代码']
    # stock_em_gpzy_pledge_ratio_df = stock_gpzy_pledge_ratio_em(date="20240331")
    # print(stock_em_gpzy_pledge_ratio_df)

    # Index(['序号', '股票代码', '股票简称', '股东名称', '质押股份数量', '占所持股份比例', '占总股本比例', '质押机构',
    #        '最新价', '质押日收盘价', '预估平仓线', '质押开始日期', '公告日期'],
    #       dtype='object')
    # stock_gpzy_pledge_ratio_detail_em_df = stock_gpzy_pledge_ratio_detail_page1_em()
    # print(stock_gpzy_pledge_ratio_detail_em_df)

    init_log("em_stock_gpzy.log")

    record_stock_data()

    # sched.start()
    #
    # sched._thread.join()
    # stock_em_gpzy_distribute_statistics_company_df = (
    #     stock_gpzy_distribute_statistics_company_em()
    # )
    # print(stock_em_gpzy_distribute_statistics_company_df)

    # stock_em_gpzy_distribute_statistics_bank_df = (
    #     stock_gpzy_distribute_statistics_bank_em()
    # )
    # print(stock_em_gpzy_distribute_statistics_bank_df)
    #
    # stock_gpzy_industry_data_em_df = stock_gpzy_industry_data_em()
    # print(stock_gpzy_industry_data_em_df)
