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



print("✅ 修正后的关键路径：")
print(f"[0] {sys.path[0]}")  # 应显示 D:\source\zvtm\zvtm




logger = logging.getLogger(__name__)
sched = BackgroundScheduler()




from bs4 import BeautifulSoup
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/114.0.0.0 Safari/537.36"
}


def record_stock_data():
    email_action = EmailInformer()
    msg = f"record stock1d success,数据来源: em"
    try:
        # 获取股票代码
        stock_a_indicator_lg_all_df = stock_a_indicator_lg(symbol="all")
        # print(stock_a_indicator_lg_all_df)
       #  df = pd.DataFrame(columns=['日期', '股票代码', 'entity', '名称', '开盘', '收盘', '最高', '最低', '成交量', '成交额',
       # '振幅', '涨跌幅', '涨跌额', '换手率'])
        list_of_df = []
        dt = datetime.datetime.now().strftime('%Y%m%d')
        for code in stock_a_indicator_lg_all_df['code']:
            df_code = stock_zh_a_hist(
                symbol=code,
                period="daily",
                start_date=dt,
                end_date=dt,
                adjust="hfq",
            )
            list_of_df.append(df_code)
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
from schedule.utils.query_data import get_data
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
