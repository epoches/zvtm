# -*- coding: utf-8 -*-

import pandas as pd
from zvtm import zvt_config

from pandas._libs.tslibs.timedeltas import Timedelta

from zvtm.contract.api import df_to_db
from zvtm.contract.recorder import TimeSeriesDataRecorder
from zvtm.domain import Stock, StockValuation1
import requests
import pandas as pd
from zvtm.utils.time_utils import now_pd_timestamp, to_time_str, to_pd_timestamp
from zvtm.utils import pd_is_not_null

class EmChinaStockValuationRecorder(TimeSeriesDataRecorder):
    entity_provider = "em"
    entity_schema = Stock

    # 数据来自em
    provider = 'em'

    data_schema = StockValuation1



    def init_entities(self):
        pass
        super().init_entities()
        # 过滤掉退市的
        self.entities = [entity for entity in self.entities if
                         (entity.end_date is None) or (entity.end_date > now_pd_timestamp())]
        url = "http://82.push2.eastmoney.com/api/qt/clist/get"
        params = {
            "pn": "1",
            "pz": "50000",
            "po": "1",
            "np": "1",
            "ut": "bd1d9ddb04089700cf9c27f6f7426281",
            "fltt": "2",
            "invt": "2",
            "fid": "f3",
            "fs": "m:0 t:6,m:0 t:80,m:1 t:2,m:1 t:23",
            "fields": "f9,f10,f12,f13,f14,f20,f21,f23,f114,f115,f124,f130,f131",
            "_": "1623833739532",
        }
        r = requests.get(url, params=params)
        data_json = r.json()
        if not data_json["data"]["diff"]:
            return pd.DataFrame()
        temp_df = pd.DataFrame(data_json["data"]["diff"])
        temp_df.columns = [
            "pe_ttm1",
            "lb",
            "code",
            "entity",
            "name",
            "market_cap",
            "circulating_market_cap",
            "pb",
            "pe",
            "pe_ttm",
            "timestamp",
            "ps",
            "pcf",
        ]
        # temp_df.reset_index(inplace=True)
        # temp_df["index"] = temp_df.index + 1
        # temp_df.rename(columns={"index": "序号"}, inplace=True)
        temp_df = temp_df[
            [
                "pe_ttm1",
                "lb",
                "code",
                "entity",
                "name",
                "market_cap",
                "circulating_market_cap",
                "pb",
                "pe",
                "pe_ttm",
                "timestamp",
                "ps",
                "pcf",
            ]
        ]
        temp_df["entity"] = temp_df["entity"].apply(lambda x: 'sz' if x == 0 else 'sh')
        temp_df["lb"] = pd.to_numeric(temp_df["lb"], errors="coerce")
        temp_df["pb"] = pd.to_numeric(temp_df["pb"], errors="coerce")
        dt = pd.to_datetime(temp_df["timestamp"], unit='s', errors="coerce")[0].replace(hour=0, minute=0, second=0)
        temp_df["timestamp"] = dt
        temp_df["pe"] = pd.to_numeric(temp_df["pe"], errors="coerce")
        temp_df["pe_ttm1"] = pd.to_numeric(temp_df["pe_ttm1"], errors="coerce")
        temp_df["ps"] = pd.to_numeric(temp_df["ps"], errors="coerce")
        temp_df["pcf"] = pd.to_numeric(temp_df["pcf"], errors="coerce")
        temp_df["pe_ttm"] = pd.to_numeric(temp_df["pe_ttm"], errors="coerce")
        temp_df["market_cap"] = pd.to_numeric(temp_df["market_cap"], errors="coerce")
        temp_df["circulating_market_cap"] = pd.to_numeric(temp_df["circulating_market_cap"], errors="coerce")

        for i in range(len(temp_df)):
            entity_id = "{}_{}_{}".format('stock', temp_df.loc[i, "entity"], temp_df.loc[i, "code"])
            temp_df.loc[i, "entity_id"] = entity_id
            temp_df.loc[i, "id"] = "{}_{}".format(to_time_str(entity_id),
                                                  temp_df.loc[i, "timestamp"].strftime('%Y-%m-%d'))
        self.df = temp_df

    def record(self, entity, start, end, size, timestamps):
        # start = max(start, to_pd_timestamp("2005-01-01"))
        # end = min(now_pd_timestamp(), start + Timedelta(days=500))

        # count: Timedelta = end - start
        temp = self.df
        df = temp[temp['code'] == entity.code]

        if pd_is_not_null(df):
            df_to_db(df=df, data_schema=self.data_schema, provider=self.provider, force_update=self.force_update)
        else:
            self.logger.info(f"no kdata for {entity.id}")

        return None


if __name__ == "__main__":
    # entity_ids = ["stock_sz_300999"],
    EmChinaStockValuationRecorder(force_update=True,sleeping_time=0).run()
# the __all__ is generated
__all__ = ["EmChinaStockValuationRecorder"]
