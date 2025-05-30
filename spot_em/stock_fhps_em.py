#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
Date: 2023/4/7 15:22
Desc: 东方财富网-数据中心-年报季报-分红送配
https://data.eastmoney.com/yjfp/
"""

import pandas as pd
import requests
from tqdm import tqdm
#!/usr/bin/env python
# -*- coding:utf-8 -*-

import pandas as pd
import requests
from tqdm import tqdm
from sqlalchemy import create_engine, Column, Integer, String, Float, Date, Numeric, insert
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime, timedelta
from zvtm import zvt_config
# 数据库配置信息
DATABASE_URI = 'mysql+pymysql://' + zvt_config['mysql_user'] + ':' + zvt_config['mysql_password'] + '@' + \
                   zvt_config['mysql_host'] + ':' + zvt_config['mysql_port'] + '/eastmoney_finance'
Base = declarative_base()

# 定义数据库表模型
class StockFhps(Base):
    __tablename__ = 'stock_fhps'

    id = Column(String(length=128), primary_key=True)
    代码 = Column(String(20), nullable=False)
    名称 = Column(String(100), nullable=False)
    送转股份_送转总比例 = Column(Float)
    送转股份_送转比例 = Column(Float)
    送转股份_转股比例 = Column(Float)
    现金分红_现金分红比例 = Column(Float)
    现金分红_股息率 = Column(Float)
    每股收益 = Column(Float)
    每股净资产 = Column(Float)
    每股公积金 = Column(Float)
    每股未分配利润 = Column(Float)
    净利润同比增长 = Column(Float)
    总股本 = Column(Float)
    预案公告日 = Column(Date)
    股权登记日 = Column(Date)
    除权除息日 = Column(Date)
    方案进度 = Column(String(50))
    最新公告日期 = Column(Date)

    def __repr__(self):
        return f"<StockFhps(代码={self.代码}, 名称={self.名称})>"


def stock_fhps_em(date: str = "20231231") -> pd.DataFrame:
    """
    东方财富网-数据中心-年报季报-分红送配
    https://data.eastmoney.com/yjfp/
    :param date: 分红送配报告期
    :type date: str
    :return: 分红送配
    :rtype: pandas.DataFrame
    """
    import warnings

    warnings.simplefilter(action="ignore", category=FutureWarning)

    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
    params = {
        "sortColumns": "PLAN_NOTICE_DATE",
        "sortTypes": "-1",
        "pageSize": "500",
        "pageNumber": "1",
        "reportName": "RPT_SHAREBONUS_DET",
        "columns": "ALL",
        "quoteColumns": "",
        "js": '{"data":(x),"pages":(tp)}',
        "source": "WEB",
        "client": "WEB",
        "filter": f"""(REPORT_DATE='{"-".join([date[:4], date[4:6], date[6:]])}')""",
    }

    r = requests.get(url, params=params)
    data_json = r.json()
    total_pages = int(data_json["result"]["pages"])
    big_df = pd.DataFrame()
    # tqdm = get_tqdm()
    for page in tqdm(range(1, total_pages + 1), leave=False):
        params.update({"pageNumber": page})
        r = requests.get(url, params=params)
        data_json = r.json()
        temp_df = pd.DataFrame(data_json["result"]["data"])
        big_df = pd.concat(objs=[big_df, temp_df], ignore_index=True)

    big_df.columns = [
        "_",
        "名称",
        "_",
        "_",
        "代码",
        "送转股份-送转总比例",
        "送转股份-送转比例",
        "送转股份-转股比例",
        "现金分红-现金分红比例",
        "预案公告日",
        "股权登记日",
        "除权除息日",
        "_",
        "方案进度",
        "_",
        "最新公告日期",
        "_",
        "_",
        "_",
        "每股收益",
        "每股净资产",
        "每股公积金",
        "每股未分配利润",
        "净利润同比增长",
        "总股本",
        "_",
        "现金分红-股息率",
        "-",
        "-",
        "-",
    ]
    big_df = big_df[
        [
            "代码",
            "名称",
            "送转股份-送转总比例",
            "送转股份-送转比例",
            "送转股份-转股比例",
            "现金分红-现金分红比例",
            "现金分红-股息率",
            "每股收益",
            "每股净资产",
            "每股公积金",
            "每股未分配利润",
            "净利润同比增长",
            "总股本",
            "预案公告日",
            "股权登记日",
            "除权除息日",
            "方案进度",
            "最新公告日期",
        ]
    ]
    big_df["送转股份-送转总比例"] = pd.to_numeric(
        big_df["送转股份-送转总比例"], errors="coerce"
    )
    big_df["送转股份-送转比例"] = pd.to_numeric(
        big_df["送转股份-送转比例"], errors="coerce"
    )
    big_df["送转股份-转股比例"] = pd.to_numeric(
        big_df["送转股份-转股比例"], errors="coerce"
    )
    big_df["现金分红-现金分红比例"] = pd.to_numeric(
        big_df["现金分红-现金分红比例"], errors="coerce"
    )
    big_df["现金分红-股息率"] = pd.to_numeric(
        big_df["现金分红-股息率"], errors="coerce"
    )
    big_df["每股收益"] = pd.to_numeric(big_df["每股收益"], errors="coerce")
    big_df["每股净资产"] = pd.to_numeric(big_df["每股净资产"], errors="coerce")
    big_df["每股公积金"] = pd.to_numeric(big_df["每股公积金"], errors="coerce")
    big_df["每股未分配利润"] = pd.to_numeric(big_df["每股未分配利润"], errors="coerce")
    big_df["净利润同比增长"] = pd.to_numeric(big_df["净利润同比增长"], errors="coerce")
    big_df["总股本"] = pd.to_numeric(big_df["总股本"], errors="coerce")

    big_df["预案公告日"] = pd.to_datetime(big_df["预案公告日"], errors="coerce").dt.date
    big_df["股权登记日"] = pd.to_datetime(big_df["股权登记日"], errors="coerce").dt.date
    big_df["除权除息日"] = pd.to_datetime(big_df["除权除息日"], errors="coerce").dt.date
    big_df["id"] = big_df['代码'] + '_' + big_df['最新公告日期']
    big_df["最新公告日期"] = pd.to_datetime(
        big_df["最新公告日期"], errors="coerce"
    ).dt.date
    big_df.sort_values(["最新公告日期"], inplace=True, ignore_index=True)

    return big_df


def stock_fhps_detail_em(symbol: str = "300073") -> pd.DataFrame:
    """
    东方财富网-数据中心-分红送配-分红送配详情
    https://data.eastmoney.com/yjfp/detail/300073.html
    :param symbol: 股票代码
    :type symbol: str
    :return: 分红送配详情
    :rtype: pandas.DataFrame
    """
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
    params = {
        "sortColumns": "REPORT_DATE",
        "sortTypes": "-1",
        "pageSize": "500",
        "pageNumber": "1",
        "reportName": "RPT_SHAREBONUS_DET",
        "columns": "ALL",
        "quoteColumns": "",
        "js": '{"data":(x),"pages":(tp)}',
        "source": "WEB",
        "client": "WEB",
        "filter": f"""(SECURITY_CODE="{symbol}")""",
    }

    r = requests.get(url, params=params)
    data_json = r.json()
    total_pages = int(data_json["result"]["pages"])
    big_df = pd.DataFrame()
    tqdm = get_tqdm()
    for page in tqdm(range(1, total_pages + 1), leave=False):
        params.update({"pageNumber": page})
        r = requests.get(url, params=params)
        data_json = r.json()
        temp_df = pd.DataFrame(data_json["result"]["data"])
        big_df = pd.concat(objs=[big_df, temp_df], ignore_index=True)

    big_df.columns = [
        "_",
        "-",
        "_",
        "_",
        "-",
        "送转股份-送转总比例",
        "送转股份-送股比例",
        "送转股份-转股比例",
        "现金分红-现金分红比例",
        "业绩披露日期",
        "股权登记日",
        "除权除息日",
        "报告期",
        "方案进度",
        "现金分红-现金分红比例描述",
        "最新公告日期",
        "-",
        "-",
        "-",
        "每股收益",
        "每股净资产",
        "每股公积金",
        "每股未分配利润",
        "净利润同比增长",
        "总股本",
        "预案公告日",
        "现金分红-股息率",
        "-",
        "-",
        "-",
    ]
    big_df = big_df[
        [
            "报告期",
            "业绩披露日期",
            "送转股份-送转总比例",
            "送转股份-送股比例",
            "送转股份-转股比例",
            "现金分红-现金分红比例",
            "现金分红-现金分红比例描述",
            "现金分红-股息率",
            "每股收益",
            "每股净资产",
            "每股公积金",
            "每股未分配利润",
            "净利润同比增长",
            "总股本",
            "预案公告日",
            "股权登记日",
            "除权除息日",
            "方案进度",
            "最新公告日期",
        ]
    ]
    big_df["报告期"] = pd.to_datetime(big_df["报告期"], errors="coerce").dt.date
    big_df["业绩披露日期"] = pd.to_datetime(
        big_df["业绩披露日期"], errors="coerce"
    ).dt.date
    big_df["预案公告日"] = pd.to_datetime(big_df["预案公告日"], errors="coerce").dt.date
    big_df["股权登记日"] = pd.to_datetime(big_df["股权登记日"], errors="coerce").dt.date
    big_df["除权除息日"] = pd.to_datetime(big_df["除权除息日"], errors="coerce").dt.date
    big_df["最新公告日期"] = pd.to_datetime(
        big_df["最新公告日期"], errors="coerce"
    ).dt.date

    big_df["送转股份-送转总比例"] = pd.to_numeric(
        big_df["送转股份-送转总比例"], errors="coerce"
    )
    big_df["送转股份-送股比例"] = pd.to_numeric(
        big_df["送转股份-送股比例"], errors="coerce"
    )
    big_df["送转股份-转股比例"] = pd.to_numeric(
        big_df["送转股份-转股比例"], errors="coerce"
    )
    big_df["现金分红-现金分红比例"] = pd.to_numeric(
        big_df["现金分红-现金分红比例"], errors="coerce"
    )
    big_df["现金分红-股息率"] = pd.to_numeric(
        big_df["现金分红-股息率"], errors="coerce"
    )
    big_df["每股收益"] = pd.to_numeric(big_df["每股收益"], errors="coerce")
    big_df["每股净资产"] = pd.to_numeric(big_df["每股净资产"], errors="coerce")
    big_df["每股公积金"] = pd.to_numeric(big_df["每股公积金"], errors="coerce")
    big_df["每股未分配利润"] = pd.to_numeric(big_df["每股未分配利润"], errors="coerce")
    big_df["净利润同比增长"] = pd.to_numeric(big_df["净利润同比增长"], errors="coerce")
    big_df["总股本"] = pd.to_numeric(big_df["总股本"], errors="coerce")
    big_df.sort_values(["报告期"], inplace=True, ignore_index=True)
    return big_df



# 将数据保存到MySQL数据库
def save_to_mysql(df: pd.DataFrame):
    # 创建数据库引擎
    engine = create_engine(DATABASE_URI, echo=False)

    # 创建表（如果表不存在）
    Base.metadata.create_all(engine)

    # 创建 Session 类型
    Session = sessionmaker(bind=engine)

    # 创建 Session 实例
    session = Session()
    # 将DataFrame中的所有NaN值替换为None
    df = df.where(pd.notnull(df), None)

    for index, row in df.iterrows():
        # 创建插入语句
        stmt = (
            insert(StockFhps.__table__).
                values(
                    id=row['id'],
                    代码=row['代码'],
                    名称=row['名称'],
                    送转股份_送转总比例=row['送转股份-送转总比例'],
                    送转股份_送转比例=row['送转股份-送转比例'],
                    送转股份_转股比例=row['送转股份-转股比例'],
                    现金分红_现金分红比例=row['现金分红-现金分红比例'],
                    现金分红_股息率=row['现金分红-股息率'],
                    每股收益=row['每股收益'],
                    每股净资产=row['每股净资产'],
                    每股公积金=row['每股公积金'],
                    每股未分配利润=row['每股未分配利润'],
                    净利润同比增长=row['净利润同比增长'],
                    总股本=row['总股本'],
                    预案公告日=row['预案公告日'],
                    股权登记日=row['股权登记日'],
                    除权除息日=row['除权除息日'],
                    方案进度=row['方案进度'],
                    最新公告日期=row['最新公告日期']
                ).
            prefix_with("IGNORE")  # 添加IGNORE关键字
        )
        # 执行插入语句
        session.execute(stmt)
    session.commit()

    # 关闭 Session
    session.close()

if __name__ == "__main__":
    # import pandas as pd
    # from datetime import datetime, timedelta
    #
    # # 假设 stock_fhps_em 和 save_to_mysql 函数已经定义好
    #
    # start_date = datetime(1999, 12, 31)  # 循环开始日期
    # end_date = datetime(2023, 12, 31)  # 循环结束日期
    #
    # current_date = start_date
    # while current_date <= end_date:
    #     # 格式化日期为 YYYYMMDD 格式
    #     date_str = current_date.strftime('%Y%m%d')
    #     print(f"正在处理日期: {date_str}")
    #
    #     # 获取数据
    #     df = stock_fhps_em(date=date_str)
    #
    #     # 保存数据到MySQL
    #     save_to_mysql(df)
    #
    #     # 计算下一个季度的最后一天
    #     next_date = current_date + timedelta(days=1)
    #     # 调整到下一年的12月31日，如果是12月则调整到当年的12月31日
    #     # if current_date.month == 12 and current_date.day == 31:
    #     #     current_date = next_date
    #     # else:
    #     current_date = current_date.replace(month=12, day=31, year=next_date.year)
    # 获取数据
    df = stock_fhps_em(date="20250331")
    print(df.tail())
    # 保存数据到MySQL
    save_to_mysql(df)


    # stock_fhps_em_df = stock_fhps_em(date="19961231")
    # print(stock_fhps_em_df)

    # stock_fhps_detail_em_df = stock_fhps_detail_em(symbol="000625")
    # print(stock_fhps_detail_em_df)
