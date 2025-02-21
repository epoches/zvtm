# -*- coding: utf-8 -*-

import io

import pandas as pd
import requests

from zvtm.contract.api import df_to_db
from zvtm.contract.recorder import Recorder
from zvtm.domain import Stock, StockDetail
from zvtm.recorders.consts import DEFAULT_SH_HEADER, DEFAULT_SZ_HEADER
from zvtm.utils.time_utils import to_pd_timestamp
from ak.stock.stock_info import stock_info_sh_name_code,stock_info_sz_name_code

class ExchangeStockMetaRecorder(Recorder):
    data_schema = Stock
    provider = "exchange"

    original_page_url = "http://www.sse.com.cn/assortment/stock/list/share/"

    # 处理上海市场数据
    def process_sh_stocks(self):
        # 处理主板A股
        sh_main = stock_info_sh_name_code(symbol="主板A股").rename(columns={
            '证券代码': 'code',
            '证券简称': 'name',
            '上市日期': 'list_date'
        })[["code", "name", "list_date"]]

        # 处理科创板
        kcb = stock_info_sh_name_code(symbol="科创板").rename(columns={
            '证券代码': 'code',
            '证券简称': 'name',
            '上市日期': 'list_date'
        })[["code", "name", "list_date"]]

        # 合并数据
        return pd.concat([sh_main, kcb], ignore_index=True)

    # 处理深圳市场数据
    def process_sz_stocks(self):
        return stock_info_sz_name_code(symbol="A股列表").rename(columns={
            'A股代码': 'code',
            'A股简称': 'name',
            'A股上市日期': 'list_date'
        })[["code", "name", "list_date"]]

    def run(self):
        # url = (
        #     "http://query.sse.com.cn/security/stock/downloadStockListFile.do?csrcCode=&stockCode=&areaName=&stockType=1"
        # )
        # resp = requests.get(url, headers=DEFAULT_SH_HEADER)
        # self.download_stock_list(response=resp, exchange="sh")
        #
        # url = (
        #     "http://query.sse.com.cn/security/stock/downloadStockListFile.do?csrcCode=&stockCode=&areaName=&stockType=8"
        # )
        # resp = requests.get(url, headers=DEFAULT_SH_HEADER)
        # self.download_stock_list(response=resp, exchange="sh")
        #
        # url = "http://www.szse.cn/api/report/ShowReport?SHOWTYPE=xlsx&CATALOGID=1110&TABKEY=tab1&random=0.20932135244582617"
        # resp = requests.get(url, headers=DEFAULT_SZ_HEADER)
        # self.download_stock_list(response=resp, exchange="sz")
        sh_df = self.process_sh_stocks()
        sz_df = self.process_sz_stocks()

        self.save_stock_list(df=sh_df, exchange="sh")
        self.save_stock_list(df=sz_df, exchange="sz")

    def save_stock_list(self,df,exchange="sh"):
        if df is not None:
            # 动态处理列
            required_columns = ["code", "name", "list_date"]
            df = df.loc[:, required_columns].copy()

            # 统一处理日期格式
            df["list_date"] = pd.to_datetime(df["list_date"], errors='coerce')

            # 添加元数据
            df = df.assign(
                exchange=exchange,
                entity_type="stock",
                timestamp=df["list_date"]
            ).dropna(subset=["list_date"])

            # 生成唯一标识
            df["id"] = df.apply(lambda x: f"{x.entity_type}_{x.exchange}_{x.code}", axis=1)
            df = df.drop_duplicates("id", keep="last")

            # 分步存储
            try:
                df_to_db(df=df, data_schema=self.data_schema, provider=self.provider, force_update=True)
                df_to_db(df=df, data_schema=StockDetail, provider=self.provider, force_update=True)
                self.logger.info(f"Persisted {len(df)} records to {exchange}")
            except Exception as e:
                self.logger.error(f"Persist failed: {str(e)}")
                raise

    def download_stock_list(self, response, exchange):
        df = None
        if exchange == "sh":
            df = pd.read_csv(
                io.BytesIO(response.content),
                sep="\t+",
                encoding="GB2312",
                dtype=str,
                parse_dates=["上市日期"],
                # date_format="%Y-%m-%d",
                # on_bad_lines="skip",
            )
            print(df)

            if df is not None:
                # 根据固定列位置选择（需确保数据格式稳定）
                df = df.iloc[:, [0, 1, 4]]  # 选择第1、2、5列
                df.columns = ["公司代码", "公司简称", "上市日期"]  # 强制重命名

                # df = df.loc[:, ["公司代码", "公司简称", "上市日期"]]

        elif exchange == "sz":
            df = pd.read_excel(
                io.BytesIO(response.content),
                sheet_name="A股列表",
                dtype=str,
                parse_dates=["A股上市日期"],
                # date_format="%Y-m-d",
            )
            if df is not None:
                df = df.loc[:, ["A股代码", "A股简称", "A股上市日期"]]

        if df is not None:
            df.columns = ["code", "name", "list_date"]

            df = df.dropna(subset=["code"])

            # handle the dirty data
            # 600996,贵广网络,2016-12-26,2016-12-26,sh,stock,stock_sh_600996,,次新股,贵州,,
            df.loc[df["code"] == "600996", "list_date"] = "2016-12-26"
            print(df[df["list_date"] == "-"])
            print(df["list_date"])
            df["list_date"] = df["list_date"].apply(lambda x: to_pd_timestamp(x))
            df["exchange"] = exchange
            df["entity_type"] = "stock"
            df["id"] = df[["entity_type", "exchange", "code"]].apply(lambda x: "_".join(x.astype(str)), axis=1)
            df["entity_id"] = df["id"]
            df["timestamp"] = df["list_date"]
            df = df.dropna(axis=0, how="any")
            df = df.drop_duplicates(subset=("id"), keep="last")
            df_to_db(df=df, data_schema=self.data_schema, provider=self.provider, force_update=True)
            # persist StockDetail too
            df_to_db(df=df, data_schema=StockDetail, provider=self.provider, force_update=True)
            self.logger.info(df.tail())
            self.logger.info("persist stock list successs")


__all__ = ["ExchangeStockMetaRecorder"]

if __name__ == "__main__":
    recorder = ExchangeStockMetaRecorder()
    recorder.run()


# the __all__ is generated
__all__ = ["ExchangeStockMetaRecorder"]
