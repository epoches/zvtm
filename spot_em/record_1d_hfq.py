# -*- coding: utf-8 -*-
import sys
sys.path.insert(0, sys.path[0]+"/../")
from  zvtm.ak.stock_feature.stock_hist_em import stock_zh_a_hist
from zvtm.ak.stock_feature.stock_a_indicator import stock_a_indicator_lg
import logging
from apscheduler.schedulers.background import BackgroundScheduler
from zvtm import init_log
import requests
import pandas as pd
from zvtm.utils.time_utils import now_pd_timestamp, to_time_str, to_pd_timestamp
from zvtm.utils.pd_utils import pd_is_not_null
from zvtm.contract.api import get_db_engine,get_schema_columns
from zvtm.domain import Stock1dHfqKdata
from zvtm.contract.api import df_to_db
import datetime
from zvtm.informer import EmailInformer
from zvtm import zvt_config

# print("✅ 修正后的关键路径：")
# print(f"[0] {sys.path[0]}")  # 应显示 D:\source\zvtm\zvtm
#
import time
logger = logging.getLogger(__name__)
sched = BackgroundScheduler()
from zvtm.utils.query_data import get_data
from zvtm.utils.mysql_pool import MysqlPool
def record_stock_data():
    email_action = EmailInformer()
    msg = f"record stock1d success,数据来源: em"
    try:
        # 获取股票代码
        # stock_a_indicator_lg_all_df = stock_a_indicator_lg(symbol="all")
        db = 'exchange_stock_meta'
        mp = MysqlPool(zvt_config['mysql_host'], zvt_config['mysql_user'], zvt_config['mysql_password'], db,
                       int(zvt_config['mysql_port']), 'utf8')
        sql = 'select code,name,exchange from stock'
        arg = []
        stocks_list = mp.fetch_all(sql=sql, args=arg)
        # print(stock_a_indicator_lg_all_df)
       #  df = pd.DataFrame(columns=['日期', '股票代码', 'entity', '名称', '开盘', '收盘', '最高', '最低', '成交量', '成交额',
       # '振幅', '涨跌幅', '涨跌额', '换手率'])
        list_of_df = []
        dt = datetime.datetime.now().strftime('%Y%m%d')
        for code in stocks_list['code']:
            try:
                df_code = stock_zh_a_hist(
                        symbol=code,
                        period="daily",
                        start_date='20250524',
                        end_date=dt,
                        adjust="hfq",
                )
                time.sleep(3)
                print(f"Successfully fetched data for {code}")
                list_of_df.append(df_code)
            except Exception as e:
                print(f"Error fetching data for {code}: {e}")
                continue

            #
            # df_code = stock_zh_a_hist(
            #     symbol=code,
            #     period="daily",
            #     start_date='20250524',
            #     end_date=dt,
            #     adjust="hfq",
            # )
            # time.sleep(3)
            # print(code)
            # list_of_df.append(df_code)
        df = pd.concat(list_of_df, ignore_index=True)
        # concat数据
        force_update=True
        data_schema = Stock1dHfqKdata
        provider ='em'
        db_engine = get_db_engine(provider, data_schema=data_schema)
        schema_cols = get_schema_columns(data_schema)
        cols = set(df.columns.tolist()) & set(schema_cols)
        df.rename(columns={"日期": "timestamp"}, inplace=True)
        df.rename(columns={"股票代码": "code"}, inplace=True)
        df.rename(columns={"名称": "name"}, inplace=True)
        df.rename(columns={"收盘": "close"}, inplace=True)
        df.rename(columns={"最高": "high"}, inplace=True)
        df.rename(columns={"开盘": "open"}, inplace=True)
        df.rename(columns={"最低": "low"}, inplace=True)
        df.rename(columns={"成交量": "volume"}, inplace=True)
        df.rename(columns={"成交额": "turnover"}, inplace=True)
        df.rename(columns={"涨跌幅": "change_pct"}, inplace=True)
        df.rename(columns={"换手率": "turnover_rate"}, inplace=True)


        # dt = datetime.datetime.now().strftime('%Y-%m-%d')
        for i in range(len(df)):
            code_value = df['code'].iloc[i]
            match = stocks_list[stocks_list['code'] == code_value]

            if not match.empty:
                df.loc[i, "entity"] = match['exchange'].iloc[0]
                df.loc[i, "name"] = match['name'].iloc[0]
            else:
                # 处理未找到的情况，例如设为None或记录日志
                df.loc[i, "entity"] = None
                logger.info(f"{code_value} 未找到匹配entity")
            # df.loc[i, "entity"] = stocks_list[stocks_list['code'] == df['code'].iloc[i]]['exchange'].iloc[0]
            entity_id = "{}_{}_{}".format('stock',df.loc[i,"entity"],df.loc[i,"code"])
            df.loc[i,"entity_id"] = entity_id
            df.loc[i,"id"] = "{}_{}".format(to_time_str(entity_id),df.loc[i,"timestamp"])
            df.loc[i,"timestamp"] = df.loc[i,"timestamp"]
            df.loc[i, "level"] = "1d"
            df.loc[i, "provider"] = "em"
        df = df[schema_cols]
        if pd_is_not_null(df):
            df_to_db(df=df, data_schema=data_schema, provider=provider, force_update=force_update)
        logger.info(msg)
        email_action.send_message(zvt_config["email_username"], msg, msg)
    except Exception as e:
        logger.exception("report error:{}".format(e))
        email_action.send_message(
            zvt_config["email_username"],
            f"record stock1d error",
            f"record stock1d error: {e}",
        )

@sched.scheduled_job('cron',day_of_week='mon-fri', hour=15, minute=5)
def isopen():
    dt = datetime.datetime.now().strftime('%Y-%m-%d')
    # record_stock_data()
    db = 'tushare'
    sql = "select timestamp from trade_day where timestamp = %s "
    arg = [dt]
    df = get_data(db=db, sql=sql, arg=arg)
    if len(df) > 0:
        record_stock_data()

if __name__ == "__main__":
    init_log("em_kdatahfq1d_runner.log")

    isopen()
    # record_stock_data()
    sched.start()

    sched._thread.join()



    # stock_zh_a_hist_df = stock_zh_a_hist(
    #     symbol="000001",
    #     period="daily",
    #     start_date="20240717",
    #     end_date="20240717",
    #     adjust="hfq",
    # )
    # print(stock_zh_a_hist_df)
