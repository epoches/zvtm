# -*- coding: utf-8 -*-
import pandas as pd
from jqdatapy.api import get_all_securities, run_query

from zvtm.api.portfolio import portfolio_relate_stock
from zvtm.api.utils import china_stock_code_to_id
from zvtm.contract.api import df_to_db, get_entity_exchange, get_entity_code
from zvtm.contract.recorder import Recorder, TimeSeriesDataRecorder
from zvtm.domain import EtfStock, Stock, Etf, StockDetail,Index,IndexStock
from zvtm.recorders.joinquant.common import to_entity_id, jq_to_report_period
from zvtm.utils.pd_utils import pd_is_not_null
from zvtm.utils.time_utils import to_time_str

from jqdatasdk import get_index_stocks,auth,get_index_weights
from zvtm import zvt_config

class BaseJqChinaMetaRecorder(Recorder):
    provider = 'joinquant'

    def __init__(self, force_update=True, sleeping_time=10) -> None:
        super().__init__(force_update, sleeping_time)

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


class JqChinaStockRecorder(BaseJqChinaMetaRecorder):
    data_schema = Stock

    def run(self):
        # 抓取股票列表
        df_stock = self.to_zvt_entity(get_all_securities(code='stock'), entity_type='stock')
        df_to_db(df_stock, data_schema=Stock, provider=self.provider, force_update=self.force_update)
        # persist StockDetail too
        df_to_db(df=df_stock, data_schema=StockDetail, provider=self.provider, force_update=self.force_update)

        # self.logger.info(df_stock)
        self.logger.info("persist stock list success")


class JqChinaEtfRecorder(BaseJqChinaMetaRecorder):
    data_schema = Etf

    def run(self):
        # 抓取etf列表
        df_index = self.to_zvt_entity(get_all_securities(code='etf'), entity_type='etf', category='etf')
        df_to_db(df_index, data_schema=Etf, provider=self.provider, force_update=self.force_update)

        # self.logger.info(df_index)
        self.logger.info("persist etf list success")


class JqChinaStockEtfPortfolioRecorder(TimeSeriesDataRecorder):
    entity_provider = 'joinquant'
    entity_schema = Etf

    # 数据来自jq
    provider = 'joinquant'

    data_schema = EtfStock

    def record(self, entity, start, end, size, timestamps):
        df = run_query(table='finance.FUND_PORTFOLIO_STOCK',
                       conditions=f'pub_date#>=#{to_time_str(start)}&code#=#{entity.code}',
                       parse_dates=None)
        if pd_is_not_null(df):
            #          id    code period_start  period_end    pub_date  report_type_id report_type  rank  symbol  name      shares    market_cap  proportion
            # 0   8640569  159919   2018-07-01  2018-09-30  2018-10-26          403003        第三季度     1  601318  中国平安  19869239.0  1.361043e+09        7.09
            # 1   8640570  159919   2018-07-01  2018-09-30  2018-10-26          403003        第三季度     2  600519  贵州茅台    921670.0  6.728191e+08        3.50
            # 2   8640571  159919   2018-07-01  2018-09-30  2018-10-26          403003        第三季度     3  600036  招商银行  18918815.0  5.806184e+08        3.02
            # 3   8640572  159919   2018-07-01  2018-09-30  2018-10-26          403003        第三季度     4  601166  兴业银行  22862332.0  3.646542e+08        1.90
            df['timestamp'] = pd.to_datetime(df['pub_date'])

            df.rename(columns={'symbol': 'stock_code', 'name': 'stock_name'}, inplace=True)
            df['proportion'] = df['proportion'] * 0.01

            df = portfolio_relate_stock(df, entity)

            df['stock_id'] = df['stock_code'].apply(lambda x: china_stock_code_to_id(x))
            df['id'] = df[['entity_id', 'stock_id', 'pub_date', 'id']].apply(lambda x: '_'.join(x.astype(str)), axis=1)
            df['report_date'] = pd.to_datetime(df['period_end'])
            df['report_period'] = df['report_type'].apply(lambda x: jq_to_report_period(x))

            df_to_db(df=df, data_schema=self.data_schema, provider=self.provider, force_update=self.force_update)

            # self.logger.info(df.tail())
            self.logger.info(f"persist etf {entity.code} portfolio success {df.iloc[-1]['pub_date']}")

        return None

class JqChinaIndexRecorder(BaseJqChinaMetaRecorder):
    data_schema = Index

    def run(self):
        # 抓取指数列表
        df_index = self.to_zvt_entity(get_all_securities(code='index'), entity_type='index', category='index')
        df_to_db(df_index, data_schema=Index, provider=self.provider, force_update=self.force_update)
        entity_type='index-stock'
        category='index_stock'
        for index in range(len(df_index['code'])):
            df_index_stock_ins = pd.DataFrame(columns=["id", "entity_id", "timestamp", "entity_type",'exchange','code','name','stock_id','stock_code','stock_name','report_period','report_date','proportion','shares','market_cap'])
            jqstock = df_index['code'][index]+'.XSHG'
            auth(zvt_config['jq_username'], zvt_config['jq_password'])
            df_index_stock = get_index_stocks(jqstock)
            if len(df_index_stock)>0:
                for istock in df_index_stock:
                    df =  Stock.query_data(provider='joinquant',code=istock[0:6])
                    df_index_stock_weight = get_index_weights(index_id=istock)
                    if pd_is_not_null(df):
                        stock_code = '-' + istock[0:6]
                        df['stock_id'] = df['entity_id']
                        df['entity_id'] = entity_type + df_index['entity_id'][index][5:] + stock_code   #   [str(x) + stock_code for x in df_index['entity_id'][index]]
                        df['id'] = df['entity_id']
                        df['entity_type'] = entity_type

                        df['stock_code'] = df['code']
                        df['stock_name'] = df['name']

                        df['code'] = istock[0:6]
                        df['name'] = df_index['name'][index]
                        #指数股票权重
                        df['proportion'] =df_index_stock_weight['weight']
                        df['report_date'] = df_index_stock_weight['date']
                        df['report_period'] = ''
                        df['shares'] = ''
                        df['market_cap'] = ''
                        #df_index_stock_ins.append(df, ignore_index=True)

                        df_index_stock_ins = pd.merge(df_index_stock_ins,df,how='left',on=["id", "entity_id", "timestamp", "entity_type",'exchange','code','name','stock_id','stock_code','stock_name','report_period','report_date','proportion','shares','market_cap'])




                df_to_db(df_index_stock_ins, data_schema=IndexStock, provider=self.provider, force_update=self.force_update)
        self.logger.info("persist index and indexstock list success")



if __name__ == '__main__':
    #JqChinaEtfRecorder().run()
    JqChinaIndexRecorder().run()

    #JqChinaStockEtfPortfolioRecorder(codes=['510050']).run()
# the __all__ is generated
__all__ = ['BaseJqChinaMetaRecorder', 'JqChinaStockRecorder', 'JqChinaEtfRecorder', 'JqChinaStockEtfPortfolioRecorder']