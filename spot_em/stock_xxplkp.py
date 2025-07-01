# 信息披露考评保存到csv
# !/usr/bin/env python
# -*- coding:utf-8 -*-
"""
Date: 2025/05/28
Desc: Crawl SZSE Main Board and GEM evaluation data from https://www.szse.cn/disclosure/supervision/check/index.html
      and save to MySQL database, only inserting new records.
"""
import requests
from bs4 import BeautifulSoup
import pandas as pd
import logging
from apscheduler.schedulers.background import BackgroundScheduler
from sqlalchemy import create_engine, Column, String, Integer, insert
from sqlalchemy.orm import sessionmaker, declarative_base
from datetime import datetime
from zvtm import init_log, zvt_config
from tqdm import tqdm


import time
import random
from math import ceil

# Initialize logging
logger = logging.getLogger(__name__)
sched = BackgroundScheduler()

# Database configuration
DATABASE_URI = (
        'mysql+pymysql://' + zvt_config['mysql_user'] + ':' + zvt_config['mysql_password'] + '@' +
        zvt_config['mysql_host'] + ':' + zvt_config['mysql_port'] + '/finance'
)

# Define SQLAlchemy Base
Base = declarative_base()


# Define database table model
class CompanyEvaluation(Base):
    __tablename__ = 'szse_company_evaluation'

    id = Column(String(length=128), primary_key=True)  # Unique ID: company_code + evaluation_year
    gsdm = Column(String(20))
    gsjc = Column(String(50))
    kpjg = Column(String(50))
    kpnd = Column(Integer)

    def __repr__(self):
        return f"<CompanyEvaluation(id={self.id}, gsdm='{self.gsdm}', gsjc='{self.gsjc}')>"



def get_szse_evaluation(stock_code):
    """
    根据股票代码查询深交所信息披露考评结果
    参数:
        stock_code (str): 6位股票代码 (如:"000002")
    返回:
        pd.DataFrame: 包含历年考评结果的DataFrame，包含[公司代码, 公司简称, 考评结果, 考评年度]列
    """
    # 构造请求URL
    url = f"https://www.szse.cn/api/report/ShowReport/data"
    params = {
        "SHOWTYPE": "JSON",
        "CATALOGID": "1760_zsn",
        "TABKEY": "tab2",
        "txtDMorJC": stock_code,
        "random": random.random()
    }

    try:
        # 发送HTTP请求
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()

        # 在主板(tab2)和创业板(tab3)结果中查找有效数据
        for tab in data:
            if tab.get('data'):
                # 找到第一个有数据的板块
                records = tab['data']
                df = pd.DataFrame(records)
                # 重命名列(英文转中文)
                df = df.rename(columns={
                    'gsdm': '公司代码',
                    'gsjc': '公司简称',
                    'kpjg': '考评结果',
                    'kpnd': '考评年度'
                })
                return df

        # 如果没有找到数据
        return pd.DataFrame(columns=['公司代码', '公司简称', '考评结果', '考评年度'])

    except requests.exceptions.RequestException as e:
        print(f"请求失败: {e}")
        return pd.DataFrame()
    except ValueError as e:
        print(f"JSON解析失败: {e}")
        return pd.DataFrame()




def fetch_szse_disclosure_evaluation(board_type="tab2"):
    """
    获取深交所主板上市公司信息披露考评数据并返回DataFrame

    返回:
    DataFrame -- 包含所有主板上市公司信息披露考评结果的数据框
                列名: ['公司代码', '公司简称', '考评结果', '考评年度']
    """
    # 基础URL参数
    base_url = "https://www.szse.cn/api/report/ShowReport/data"
    params = {
        "SHOWTYPE": "JSON",
        "CATALOGID": "1760_zsn",
        "TABKEY": board_type,  # 主板数据
    }

    # 设置请求头
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Referer": "https://www.szse.cn/disclosure/supervision/inquire/index.html",
        "Accept": "application/json, text/javascript, */*; q=0.01"
    }

    # 存储所有数据的列表
    all_data = []

    try:
        # 第一页请求获取元数据
        first_page_params = params.copy()
        first_page_params["random"] = random.random()
        response = requests.get(
            base_url,
            params=first_page_params,
            headers=headers,
            timeout=30
        )
        response.raise_for_status()  # 检查HTTP错误
        json_data = response.json()

        # 检查主板数据是否存在
        if not json_data or not isinstance(json_data, list) or len(json_data) < 1:
            raise ValueError("API返回数据结构异常")

        # 获取主板数据
        mainboard_data = json_data[0]

        # 获取分页信息
        metadata = mainboard_data.get("metadata", {})
        total_records = int(metadata.get("recordcount", 0))
        page_size = int(metadata.get("pagesize", 20))
        total_pages = ceil(total_records / page_size)

        print(f"共发现 {total_records} 条记录, {total_pages} 页数据")

        # 添加第一页数据
        all_data.extend(mainboard_data.get("data", []))

        # 循环获取所有页面数据
        for page in range(2, total_pages + 1):
            # 添加延迟避免请求过快
            time.sleep(1 + random.random())

            page_params = params.copy()
            page_params["PAGENO"] = page
            page_params["random"] = random.random()

            page_response = requests.get(
                base_url,
                params=page_params,
                headers=headers,
                timeout=30
            )
            page_response.raise_for_status()
            page_data = page_response.json()

            if page_data and isinstance(page_data, list) and len(page_data) > 0:
                all_data.extend(page_data[0].get("data", []))

            # 打印进度
            if page % 50 == 0 or page == total_pages:
                print(f"已获取第 {page}/{total_pages} 页数据...")

    except Exception as e:
        print(f"数据获取过程中出错: {str(e)}")
        # 返回已获取的部分数据
        if not all_data:
            return pd.DataFrame(columns=['公司代码', '公司简称', '考评结果', '考评年度'])

    # 转换为DataFrame
    df = pd.DataFrame(all_data)

    # 重命名列（可选）
    if not df.empty:
        df = df.rename(columns={
            "gsdm": "公司代码",
            "gsjc": "公司简称",
            "kpjg": "考评结果",
            "kpnd": "考评年度"
        })

    print(f"成功获取 {len(df)} 条记录")
    return df

def fetch_data(tabkey="tab3", max_retries=5, retry_delay=3):
    """
    分页获取指定板块的数据，优化错误处理和请求间隔
    :param tabkey: 板块标识 (tab3=创业板)
    :param max_retries: 最大重试次数
    :param retry_delay: 重试等待时间(秒)
    :return: 包含所有数据的DataFrame
    """
    base_url = "https://www.szse.cn/api/report/ShowReport/data"
    all_data = []
    page_size = 100  # 每页数据量
    page_no = 1
    total_pages = None
    session = requests.Session()

    # 设置友好的请求头
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Connection": "keep-alive",
        "Referer": "https://www.szse.cn/disclosure/supervision/dynamic/index.html",
        "X-Requested-With": "XMLHttpRequest",
    }

    # 创建进度条
    pbar = None

    while True:
        params = {
            "SHOWTYPE": "JSON",
            "CATALOGID": "1760_zsn",
            "TABKEY": tabkey,
            "random": random.random(),
            "PAGENO": page_no,
            "PAGESIZE": page_size
        }

        retry_count = 0
        success = False

        while retry_count < max_retries and not success:
            try:
                # 随机延迟，避免请求过于频繁
                delay = random.uniform(1.0, 3.0)
                time.sleep(delay)

                response = session.get(
                    base_url,
                    params=params,
                    headers=headers,
                    timeout=30
                )

                if response.status_code == 200:
                    json_data = response.json()

                    # 找到目标板块的数据
                    for item in json_data:
                        if item.get("metadata", {}).get("tabkey") == tabkey:
                            metadata = item["metadata"]

                            # 第一页获取总页数
                            if total_pages is None:
                                total_pages = metadata["pagecount"]
                                record_count = metadata["recordcount"]
                                logger.info(f"总记录数: {record_count} 总页数: {total_pages}")
                                pbar = tqdm(total=total_pages, desc=f"获取{tabkey}数据", unit="页")

                            if "data" in item:
                                all_data.extend(item["data"])
                                success = True
                            else:
                                logger.warning(f"第{page_no}页无数据")
                            break
                    else:
                        logger.warning(f"未找到{tabkey}的数据")
                else:
                    logger.warning(
                        f"请求失败: 状态码 {response.status_code}, 尝试重试 ({retry_count + 1}/{max_retries})")
            except requests.exceptions.RequestException as e:
                logger.error(f"请求异常: {e}, 尝试重试 ({retry_count + 1}/{max_retries})")
            except Exception as e:
                logger.error(f"未知错误: {e}, 尝试重试 ({retry_count + 1}/{max_retries})")

            # 重试前等待
            if not success:
                retry_count += 1
                time.sleep(retry_delay * retry_count)  # 指数退避策略

        # 更新进度条
        if pbar is not None:
            pbar.update(1)

        # 检查是否完成
        if page_no >= total_pages:
            break

        page_no += 1

    # 关闭进度条
    if pbar is not None:
        pbar.close()

    return pd.DataFrame(all_data)




@sched.scheduled_job('cron', day_of_week='mon-fri', hour=20, minute=0)
def record_evaluation_data():
    """
    Fetch and save SZSE evaluation data to database, only inserting new records.
    """
    # Create database engine
    main_df = pd.read_csv("d:\\1.csv", encoding="gbk")
    gem_df = pd.read_csv("d:\\2.csv", encoding="utf_8_sig")
    engine = create_engine(DATABASE_URI, echo=False)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        # Fetch data for Main Board and GEM
        # main_df = fetch_szse_disclosure_evaluation(board_type="tab2")
        # main_df.to_csv("d:\\1.csv", encoding="gbk", index=False)

        # df = fetch_data(tabkey="tab3")
        #
        # # 重命名列
        # column_mapping = {
        #     "gsdm": "公司代码",
        #     "gsjc": "公司简称",
        #     "kpjg": "考评结果",
        #     "kpnd": "考评年度"
        # }
        # df = df.rename(columns=column_mapping)
        #
        # # 保存到CSV文件
        # csv_file = f"d:\\2.csv"
        # df.to_csv(csv_file, index=False, encoding="utf_8_sig")


        # Combine data
        combined_df = pd.concat([main_df, gem_df], ignore_index=True)

        if combined_df.empty:
            logger.info("No new data to process")
            return False
        combined_df['id'] = combined_df['gsdm'].astype(str) + '_' + combined_df['kpnd'].astype(str)

        # Get existing IDs from database to avoid duplicates
        existing_ids = {row.id for row in session.query(CompanyEvaluation.id).all()}
        new_records = combined_df[~combined_df["id"].isin(existing_ids)]

        if new_records.empty:
            logger.info("No new records to insert")
            session.close()
            return False

        # Insert new records
        for _, row in new_records.iterrows():
            stmt = insert(CompanyEvaluation).values(
                id=row["id"],
                company_code=row["gsdm"],
                company_name=row["gsjc"],
                evaluation_result=row["kpjg"],
                evaluation_year=row["kpnd"],
            ).prefix_with("IGNORE")  # Ignore duplicates
            session.execute(stmt)

        session.commit()
        logger.info(f"Inserted {len(new_records)} new records into the database")
        session.close()
        return True

    except Exception as e:
        logger.error(f"Error in record_evaluation_data: {e}")
        session.rollback()
        session.close()
        return False


if __name__ == "__main__":
    init_log("szse_evaluation.log")
    record_evaluation_data()
    # Uncomment to enable scheduling
    # sched.start()
    # sched._thread.join()
    # 使用示例

    # # 查询万科A的信息披露考评
    # df = get_szse_evaluation("000002")
    #
    # if not df.empty:
    #     print(f"找到 {len(df)} 条记录:")
    #     print(df)
    # else:
    #     print("未找到相关信息")

    # main_df = fetch_szse_disclosure_evaluation(board_type="tab2")
    # main_df.to_csv("d:\\1.csv", index=False, encoding='utf_8_sig')
    # gem_df = fetch_szse_disclosure_evaluation(board_type="tab3")
    # gem_df.to_csv("d:\\2.csv", index=False, encoding='utf_8_sig')
    # # 获取数据
    # evaluation_df = fetch_szse_disclosure_evaluation()
    #
    # # 显示前5行
    # print("\n数据预览:")
    # print(evaluation_df.head())

    # 保存到CSV文件（可选）
    # evaluation_df.to_csv("szse_disclosure_evaluation.csv", index=False, encoding='utf_8_sig')