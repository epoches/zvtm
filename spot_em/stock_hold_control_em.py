#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
Date: 2024/5/19 18:30
Desc: 东方财富网-数据中心-特色数据-高管持股
https://data.eastmoney.com/executive/list.html
"""

import pandas as pd
import requests
from tqdm import tqdm


def stock_hold_management_detail_em() -> pd.DataFrame:
    """
    东方财富网-数据中心-特色数据-高管持股-董监高及相关人员持股变动明细
    https://data.eastmoney.com/executive/list.html
    :return: 董监高及相关人员持股变动明细
    :rtype: pandas.DataFrame
    """
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
    params = {
        "reportName": "RPT_EXECUTIVE_HOLD_DETAILS",
        "columns": "ALL",
        "quoteColumns": "",
        "filter": "",
        "pageNumber": "1",
        "pageSize": "5000",
        "sortTypes": "-1,1,1",
        "sortColumns": "CHANGE_DATE,SECURITY_CODE,PERSON_NAME",
        "source": "WEB",
        "client": "WEB",
        "p": "1",
        "pageNo": "1",
        "pageNum": "1",
        "_": "1691501763413",
    }
    r = requests.get(url, params=params)
    data_json = r.json()
    total_page = data_json["result"]["pages"]
    big_df = pd.DataFrame()
    for page in tqdm(range(1, total_page + 1), leave=False):
        params.update(
            {
                "pageNumber": page,
                "p": page,
                "pageNo": page,
                "pageNum": page,
            }
        )
        r = requests.get(url, params=params)
        data_json = r.json()
        temp_df = pd.DataFrame(data_json["result"]["data"])
        big_df = pd.concat(objs=[big_df, temp_df], ignore_index=True)

    big_df.rename(
        columns={
            "SECURITY_CODE": "代码",
            "DERIVE_SECURITY_CODE": "-",
            "SECURITY_NAME": "名称",
            "CHANGE_DATE": "日期",
            "PERSON_NAME": "变动人",
            "CHANGE_SHARES": "变动股数",
            "AVERAGE_PRICE": "成交均价",
            "CHANGE_AMOUNT": "变动金额",
            "CHANGE_REASON": "变动原因",
            "CHANGE_RATIO": "变动比例",
            "CHANGE_AFTER_HOLDNUM": "变动后持股数",
            "HOLD_TYPE": "持股种类",
            "DSE_PERSON_NAME": "董监高人员姓名",
            "POSITION_NAME": "职务",
            "PERSON_DSE_RELATION": "变动人与董监高的关系",
            "ORG_CODE": "-",
            "GGEID": "-",
            "BEGIN_HOLD_NUM": "开始时持有",
            "END_HOLD_NUM": "结束后持有",
        },
        inplace=True,
    )

    big_df = big_df[
        [
            "日期",
            "代码",
            "名称",
            "变动人",
            "变动股数",
            "成交均价",
            "变动金额",
            "变动原因",
            "变动比例",
            "变动后持股数",
            "持股种类",
            "董监高人员姓名",
            "职务",
            "变动人与董监高的关系",
            "开始时持有",
            "结束后持有",
        ]
    ]
    big_df["日期"] = pd.to_datetime(big_df["日期"], errors="coerce").dt.date
    big_df["变动股数"] = pd.to_numeric(big_df["变动股数"], errors="coerce")
    big_df["成交均价"] = pd.to_numeric(big_df["成交均价"], errors="coerce")
    big_df["变动金额"] = pd.to_numeric(big_df["变动金额"], errors="coerce")
    big_df["变动比例"] = pd.to_numeric(big_df["变动比例"], errors="coerce")
    big_df["变动后持股数"] = pd.to_numeric(big_df["变动后持股数"], errors="coerce")
    big_df["开始时持有"] = pd.to_numeric(big_df["开始时持有"], errors="coerce")
    big_df["结束后持有"] = pd.to_numeric(big_df["结束后持有"], errors="coerce")
    return big_df



def stock_hold_management_detail_page1_em() -> pd.DataFrame:
    """
    东方财富网-数据中心-特色数据-高管持股-董监高及相关人员持股变动明细
    https://data.eastmoney.com/executive/list.html
    :return: 董监高及相关人员持股变动明细
    :rtype: pandas.DataFrame
    """
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
    params = {
        "reportName": "RPT_EXECUTIVE_HOLD_DETAILS",
        "columns": "ALL",
        "quoteColumns": "",
        "filter": "",
        "pageNumber": "1",
        "pageSize": "5000",
        "sortTypes": "-1,1,1",
        "sortColumns": "CHANGE_DATE,SECURITY_CODE,PERSON_NAME",
        "source": "WEB",
        "client": "WEB",
        "p": "1",
        "pageNo": "1",
        "pageNum": "1",
        "_": "1691501763413",
    }
    r = requests.get(url, params=params)
    data_json = r.json()
    # total_page = data_json["result"]["pages"]
    big_df = pd.DataFrame(data_json["result"]["data"])
    # big_df = pd.DataFrame()
    # for page in tqdm(range(1, total_page + 1), leave=False):
    #     params.update(
    #         {
    #             "pageNumber": page,
    #             "p": page,
    #             "pageNo": page,
    #             "pageNum": page,
    #         }
    #     )
    #     r = requests.get(url, params=params)
    #     data_json = r.json()
    #     temp_df = pd.DataFrame(data_json["result"]["data"])
    #     big_df = pd.concat(objs=[big_df, temp_df], ignore_index=True)

    big_df.rename(
        columns={
            "SECURITY_CODE": "代码",
            "DERIVE_SECURITY_CODE": "-",
            "SECURITY_NAME": "名称",
            "CHANGE_DATE": "日期",
            "PERSON_NAME": "变动人",
            "CHANGE_SHARES": "变动股数",
            "AVERAGE_PRICE": "成交均价",
            "CHANGE_AMOUNT": "变动金额",
            "CHANGE_REASON": "变动原因",
            "CHANGE_RATIO": "变动比例",
            "CHANGE_AFTER_HOLDNUM": "变动后持股数",
            "HOLD_TYPE": "持股种类",
            "DSE_PERSON_NAME": "董监高人员姓名",
            "POSITION_NAME": "职务",
            "PERSON_DSE_RELATION": "变动人与董监高的关系",
            "ORG_CODE": "-",
            "GGEID": "-",
            "BEGIN_HOLD_NUM": "开始时持有",
            "END_HOLD_NUM": "结束后持有",
        },
        inplace=True,
    )

    big_df = big_df[
        [
            "日期",
            "代码",
            "名称",
            "变动人",
            "变动股数",
            "成交均价",
            "变动金额",
            "变动原因",
            "变动比例",
            "变动后持股数",
            "持股种类",
            "董监高人员姓名",
            "职务",
            "变动人与董监高的关系",
            "开始时持有",
            "结束后持有",
        ]
    ]
    big_df["日期"] = pd.to_datetime(big_df["日期"], errors="coerce").dt.date
    big_df["变动股数"] = pd.to_numeric(big_df["变动股数"], errors="coerce")
    big_df["成交均价"] = pd.to_numeric(big_df["成交均价"], errors="coerce")
    big_df["变动金额"] = pd.to_numeric(big_df["变动金额"], errors="coerce")
    big_df["变动比例"] = pd.to_numeric(big_df["变动比例"], errors="coerce")
    big_df["变动后持股数"] = pd.to_numeric(big_df["变动后持股数"], errors="coerce")
    big_df["开始时持有"] = pd.to_numeric(big_df["开始时持有"], errors="coerce")
    big_df["结束后持有"] = pd.to_numeric(big_df["结束后持有"], errors="coerce")
    return big_df


def stock_hold_management_person_em(
    symbol: str = "001308", name: str = "吴远"
) -> pd.DataFrame:
    """
    东方财富网-数据中心-特色数据-高管持股-人员增减持股变动明细
    https://data.eastmoney.com/executive/personinfo.html?name=%E5%90%B4%E8%BF%9C&code=001308
    :param symbol: 股票代码
    :type name: str
    :param name: 高管名称
    :type symbol: str
    :return: 人员增减持股变动明细
    :rtype: pandas.DataFrame
    """
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
    params = {
        "reportName": "RPT_EXECUTIVE_HOLD_DETAILS",
        "columns": "ALL",
        "quoteColumns": "",
        "filter": f'(SECURITY_CODE="{symbol}")(PERSON_NAME="{name}")',
        "pageNumber": "1",
        "pageSize": "5000",
        "sortTypes": "-1,1,1",
        "sortColumns": "CHANGE_DATE,SECURITY_CODE,PERSON_NAME",
        "source": "WEB",
        "client": "WEB",
        "_": "1691503078611",
    }
    r = requests.get(url, params=params)
    data_json = r.json()
    temp_df = pd.DataFrame(data_json["result"]["data"])
    temp_df.rename(
        columns={
            "SECURITY_CODE": "代码",
            "DERIVE_SECURITY_CODE": "-",
            "SECURITY_NAME": "名称",
            "CHANGE_DATE": "日期",
            "PERSON_NAME": "变动人",
            "CHANGE_SHARES": "变动股数",
            "AVERAGE_PRICE": "成交均价",
            "CHANGE_AMOUNT": "变动金额",
            "CHANGE_REASON": "变动原因",
            "CHANGE_RATIO": "变动比例",
            "CHANGE_AFTER_HOLDNUM": "变动后持股数",
            "HOLD_TYPE": "持股种类",
            "DSE_PERSON_NAME": "董监高人员姓名",
            "POSITION_NAME": "职务",
            "PERSON_DSE_RELATION": "变动人与董监高的关系",
            "ORG_CODE": "-",
            "GGEID": "-",
            "BEGIN_HOLD_NUM": "开始时持有",
            "END_HOLD_NUM": "结束后持有",
        },
        inplace=True,
    )

    temp_df = temp_df[
        [
            "日期",
            "代码",
            "名称",
            "变动人",
            "变动股数",
            "成交均价",
            "变动金额",
            "变动原因",
            "变动比例",
            "变动后持股数",
            "持股种类",
            "董监高人员姓名",
            "职务",
            "变动人与董监高的关系",
            "开始时持有",
            "结束后持有",
        ]
    ]
    temp_df["日期"] = pd.to_datetime(temp_df["日期"], errors="coerce").dt.date
    temp_df["变动股数"] = pd.to_numeric(temp_df["变动股数"], errors="coerce")
    temp_df["成交均价"] = pd.to_numeric(temp_df["成交均价"], errors="coerce")
    temp_df["变动金额"] = pd.to_numeric(temp_df["变动金额"], errors="coerce")
    temp_df["变动比例"] = pd.to_numeric(temp_df["变动比例"], errors="coerce")
    temp_df["变动后持股数"] = pd.to_numeric(temp_df["变动后持股数"], errors="coerce")
    temp_df["开始时持有"] = pd.to_numeric(temp_df["开始时持有"], errors="coerce")
    temp_df["结束后持有"] = pd.to_numeric(temp_df["结束后持有"], errors="coerce")
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
    # 第一页
    stock_hold_management_detail_page1_em_df = stock_hold_management_detail_page1_em()
    # 全部
    # stock_hold_management_detail_page1_em_df = stock_hold_management_detail_em()
    # print(stock_hold_management_detail_em_df)


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
    stock_hold_management_detail_page1_em_df['日期'] = pd.to_datetime(stock_hold_management_detail_page1_em_df['日期'])
    stock_hold_management_detail_page1_em_df['日期'] = stock_hold_management_detail_page1_em_df['日期'].dt.strftime('%Y-%m-%d')

    # 为每条记录创建一个唯一的序号
    stock_hold_management_detail_page1_em_df['序号'] = stock_hold_management_detail_page1_em_df['代码'] + '_' + stock_hold_management_detail_page1_em_df['日期']

    # 填充 NaN 值
    stock_hold_management_detail_page1_em_df = stock_hold_management_detail_page1_em_df.fillna(
        value=np.nan)  # 确保所有 NaNs 都被明确设置
    # 将 NaN 替换为默认值，例如 0
    stock_hold_management_detail_page1_em_df.replace({np.nan: 0}, inplace=True)

    metadata = MetaData()

    # 定义表结构
    stock_hold_management = Table(
        'stock_hold_management', metadata,
        Column('序号', String(128), primary_key=True),
        Column('日期', Date),
        Column('代码', String(20)),
        Column('名称', String(50)),
        Column('变动人', String(50)),
        Column('变动股数', Float),
        Column('成交均价', Float),
        Column('变动金额', Float),
        Column('变动原因', String(200)),
        Column('变动比例', Float),
        Column('变动后持股数', Float),
        Column('持股种类', String(50)),
        Column('董监高人员姓名', String(50)),
        Column('职务', String(50)),
        Column('变动人与董监高的关系', String(100)),
        Column('开始时持有', Float),
        Column('结束后持有', Float)
    )

    # 如果表不存在则创建表
    metadata.create_all(engine)

    try:
        # 遍历DataFrame中的每一行
        for index, row in stock_hold_management_detail_page1_em_df.iterrows():
            # 创建一个insert()语句，如果遇到主键冲突则更新记录
            stmt = (
                insert(stock_hold_management).values({
                    '序号': row['序号'],
                    '日期': row['日期'],
                    '代码': row['代码'],
                    '名称': row['名称'],
                    '变动人': row['变动人'],
                    '变动股数': row['变动股数'],
                    '成交均价': row['成交均价'],
                    '变动金额': row['变动金额'],
                    '变动原因': row['变动原因'],
                    '变动比例': row['变动比例'],
                    '变动后持股数': row['变动后持股数'],
                    '持股种类': row['持股种类'],
                    '董监高人员姓名': row['董监高人员姓名'],
                    '职务': row['职务'],
                    '变动人与董监高的关系': row['变动人与董监高的关系'],
                    '开始时持有': row['开始时持有'],
                    '结束后持有': row['结束后持有']
                })
                    .on_duplicate_key_update(  # 使用字典指定更新的列
                    {'代码': row['代码'],
                     '名称': row['名称'],
                     '变动人': row['变动人'],
                     # ... 其他列
                     '结束后持有': row['结束后持有']
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

    init_log("em_stock_hold.log")

    record_stock_data()

    sched.start()

    sched._thread.join()

    # stock_hold_management_person_em_df = stock_hold_management_person_em(
    #     symbol="001308", name="吴远"
    # )
    # print(stock_hold_management_person_em_df)
