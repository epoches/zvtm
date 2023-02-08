# -*- coding: utf-8 -*-

from typing import List

import pandas as pd


from zvtm.contract.api import df_to_db
from zvtm.contract.recorder import TimestampsDataRecorder
from zvtm.domain import Index, IndexStock
#from zvtm.recorders.exchange.api import cs_index_stock_api, cn_index_stock_api

from zvtm.utils.time_utils import pre_month_start_date

from jqdatasdk import get_index_stocks,auth,get_index_weights,get_all_securities
from zvtm import zvt_config
# from jqdatapy.api import get_all_securities, run_query
from zvtm.contract.recorder import Recorder
from zvtm.utils.pd_utils import pd_is_not_null
from zvtm.domain import EtfStock, Stock, Etf, StockDetail,Index,IndexStock
from zvtm.utils.time_utils import to_pd_timestamp, to_time_str, TIME_FORMAT_MON,TIME_FORMAT_DAY,to_timestamp
from zvtm.recorders.joinquant.common import to_entity_id
from zvtm.contract.api import df_to_db, get_entity_exchange, get_entity_code


class JqIndexRecorder(Recorder):# get_index(code, self):
    provider = 'exchange'
    data_schema = Index

    def to_zvt_entity(self, df, entity_type, category=None):
        df = df.set_index('code')
        df.index.name = 'entity_id'
        df = df.reset_index()
        # 上市日期
        df.rename(columns={'start_date': 'timestamp'}, inplace=True)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df['list_date'] = df['timestamp']
        df['end_date'] = pd.to_datetime(df['end_date'])

        df['entity_id'] = df['entity_id'].apply(lambda x: to_entity_id(entity_type=entity_type, jq_code=x))
        df['id'] = df['entity_id']
        df['entity_type'] = entity_type
        df['exchange'] = df['entity_id'].apply(lambda x: get_entity_exchange(x))
        df['code'] = df['entity_id'].apply(lambda x: get_entity_code(x))
        df['name'] = df['display_name']

        if category:
            df['category'] = category

        return df

    def run(self):
        # 抓取指数列表
        df_index = self.to_zvt_entity(get_all_securities(code='index'), entity_type='index', category='index')
        df_to_db(df_index, data_schema=Index, provider=self.provider, force_update=self.force_update)
        self.logger.info(f"finish record index:jqindex")


class JqIndexStockRecorder(TimestampsDataRecorder):
    entity_provider = 'joinquant'
    entity_schema = Index

    provider = 'joinquant'
    data_schema = IndexStock

    def __init__(self,
                 force_update=False,
                 sleeping_time=0,
                 exchanges=None,
                 entity_ids=None,
                 code=None,
                 codes=None,
                 day_data=False,
                 entity_filters=None,
                 ignore_failed=True,
                 real_time=False,
                 fix_duplicate_way='add',
                 start_timestamp=None,
                 end_timestamp=None,
                 record_history=False) -> None:
        self.code = code
        # super().__init__(force_update, sleeping_time, exchanges, entity_ids, code, codes, day_data, entity_filters,
        #                  ignore_failed, real_time, fix_duplicate_way, start_timestamp, end_timestamp)
        # self.record_history = record_history

    # def init_timestamps(self, entity_item) -> List[pd.Timestamp]:
    #     last_valid_date = pre_month_start_date()
    #     if self.record_history:
    #         # 每个月记录一次
    #         return [to_pd_timestamp(item) for item in
    #                 pd.date_range(entity_item.list_date, last_valid_date, freq='M')]
    #     else:
    #         return [last_valid_date]
    #start, end, size,  timestamps
    #for timestamp in timestamps:
    #不用init了，直接填充entity timestamp

    def run(self):#, entity,timestamp
        entity = Index.query_data(code=self.code)
        if entity.shape[0]==0:
            return
        timestamp = pd.Timestamp.now()
        df = get_index_stock(entity_item=entity, timestamp=timestamp, name=entity.name)#
        df_to_db(data_schema=self.data_schema, df=df, provider=self.provider,
                     force_update=True)#self.force_update


def get_index_stock(entity_item, timestamp, name):
    entity_type = 'index'
    jqstock = entity_item.code[0] + '.XSHG'
    auth(zvt_config['jq_username'], zvt_config['jq_password'])
    data_str = to_time_str(timestamp, TIME_FORMAT_DAY)
    #根据index指数的code编码获取包含的股票series
    df_index_stock = get_index_stocks(jqstock,date=data_str)
    df_index_stock_weight = get_index_weights(index_id=jqstock)
    the_list = []
    if len(df_index_stock) > 0:
        for istock in df_index_stock:
            result = Stock.query_data(provider='joinquant', code=istock[0:6])

            if result.shape[0] == 0:
                return
            stock_code = istock
            stock_id = result['entity_id'][0]
            exchange = result['exchange'][0]
            entity_id = f'{entity_item.entity_id[0]}_{stock_id}'
            #f'{entity_type}_{exchange}_{istock}'

            stock_name = result['name'][0]
            code = entity_item.code[0]
            name = entity_item.name[0]
            # 指数股票权重
            if df_index_stock_weight.shape[0]>0:
                report_date = df_index_stock_weight['date'][0]
                proportion = df_index_stock_weight['weight'][0]
            else:
                report_date = ''
                proportion =''
            #report_period = ''
            #shares = ''
            #'market_cap': '',  # Market capitalization/capitalisation 市场总值
            the_list.append({
                'id': '{}_{}_{}'.format(entity_item.entity_id[0], str(to_timestamp(timestamp))[:8], stock_id),
                'entity_id': entity_id,
                'entity_type': entity_type,
                'exchange': exchange,
                'code': code,
                'name': name,
                'timestamp': report_date,
                'stock_id': stock_id,
                'stock_code': stock_code[:6],
                'stock_name': stock_name,
                'proportion': proportion,
                'report_date':report_date
            })
        if the_list:
            df = pd.DataFrame.from_records(the_list)
            return df












if __name__ == '__main__':
    #JqIndexRecorder().run()
    JqIndexStockRecorder(code=['000001']).run()#code=['000001']
# the __all__ is generated
__all__ = ['JqIndexRecorder','JqIndexStockRecorder']