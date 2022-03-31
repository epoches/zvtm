# -*- coding: utf-8 -*-
import inspect
from datetime import timedelta
from typing import List, Union

import pandas as pd
from sqlalchemy import Column, String, DateTime, Float
from sqlalchemy.orm import Session

from zvtm.contract import IntervalLevel
from zvtm.utils.time_utils import date_and_time, is_same_time, now_pd_timestamp


class Mixin(object):
    """
    Base class of schema.
    """

    #: id
    id = Column(String(length=128), primary_key=True)
    #: entity id
    entity_id = Column(String(length=128))

    #: the meaning could be different for different case,most time it means 'happen time'
    timestamp = Column(DateTime)

    # unix epoch,same meaning with timestamp
    # ts = Column(BIGINT)

    @classmethod
    def help(cls):
        print(inspect.getsource(cls))

    @classmethod
    def important_cols(cls):
        return []

    @classmethod
    def time_field(cls):
        return "timestamp"

    @classmethod
    def register_recorder_cls(cls, provider, recorder_cls):
        """
        register the recorder for the schema

        :param provider:
        :param recorder_cls:
        """
        # don't make provider_map_recorder as class field,it should be created for the sub class as need
        if not hasattr(cls, "provider_map_recorder"):
            cls.provider_map_recorder = {}

        if provider not in cls.provider_map_recorder:
            cls.provider_map_recorder[provider] = recorder_cls

    @classmethod
    def register_provider(cls, provider):
        """
        register the provider to the schema defined by cls

        :param provider:
        """
        # don't make providers as class field,it should be created for the sub class as need
        if not hasattr(cls, "providers"):
            cls.providers = []

        if provider not in cls.providers:
            cls.providers.append(provider)

    @classmethod
    def get_providers(cls) -> List[str]:
        """
        providers of the schema defined by cls

        :return: providers
        """
        assert hasattr(cls, "providers")
        return cls.providers

    @classmethod
    def test_data_correctness(cls, provider, data_samples):
        for data in data_samples:
            item = cls.query_data(provider=provider, ids=[data["id"]], return_type="dict")
            print(item)
            for k in data:
                if k == "timestamp":
                    assert is_same_time(item[0][k], data[k])
                else:
                    assert item[0][k] == data[k]

    @classmethod
    def get_one(cls, id, provider_index: int = 0, provider: str = None):
        from .api import get_one

        if not provider:
            provider = cls.providers[provider_index]
        return get_one(data_schema=cls, id=id, provider=provider)

    @classmethod
    def query_data(
        cls,
        provider_index: int = 0,
        ids: List[str] = None,
        entity_ids: List[str] = None,
        entity_id: str = None,
        codes: List[str] = None,
        code: str = None,
        level: Union[IntervalLevel, str] = None,
        provider: str = None,
        columns: List = None,
        col_label: dict = None,
        return_type: str = "df",
        start_timestamp: Union[pd.Timestamp, str] = None,
        end_timestamp: Union[pd.Timestamp, str] = None,
        filters: List = None,
        session: Session = None,
        order=None,
        limit: int = None,
        index: Union[str, list] = None,
        drop_index_col=False,
        time_field: str = "timestamp",
    ):
        """
        query data by the arguments

        :param provider_index:
        :param data_schema:
        :param ids:
        :param entity_ids:
        :param entity_id:
        :param codes:
        :param code:
        :param level:
        :param provider:
        :param columns:
        :param col_label: dict with key(column), value(label)
        :param return_type: df, domain or dict. default is df
        :param start_timestamp:
        :param end_timestamp:
        :param filters:
        :param session:
        :param order:
        :param limit:
        :param index: index field name, str for single index, str list for multiple index
        :param drop_index_col: whether drop the col if it's in index, default False
        :param time_field:
        :return: results basing on return_type.
        """
        from .api import get_data

        if not provider:
            provider = cls.providers[provider_index]
        return get_data(
            data_schema=cls,
            ids=ids,
            entity_ids=entity_ids,
            entity_id=entity_id,
            codes=codes,
            code=code,
            level=level,
            provider=provider,
            columns=columns,
            col_label=col_label,
            return_type=return_type,
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
            filters=filters,
            session=session,
            order=order,
            limit=limit,
            index=index,
            drop_index_col=drop_index_col,
            time_field=time_field,
        )

    @classmethod
    def get_storages(
        cls,
        provider: str = None,
    ):
        """
        get the storages info

        :param provider: provider
        :return: storages
        """
        if not provider:
            providers = cls.get_providers()
        else:
            providers = [provider]
        from zvt.contract.api import get_db_engine

        engines = []
        for p in providers:
            engines.append(get_db_engine(provider=p, data_schema=cls))
        return engines

    @classmethod
    def record_data(
        cls,
        provider_index: int = 0,
        provider: str = None,
        force_update=None,
        sleeping_time=None,
        exchanges=None,
        entity_id=None,
        entity_ids=None,
        code=None,
        codes=None,
        real_time=None,
        fix_duplicate_way=None,
        start_timestamp=None,
        end_timestamp=None,
        one_day_trading_minutes=None,
        **kwargs,
    ):
        """
        record data by the arguments

        :param provider_index:
        :param provider:
        :param force_update:
        :param sleeping_time:
        :param exchanges:
        :param entity_ids:
        :param code:
        :param codes:
        :param real_time:
        :param fix_duplicate_way:
        :param start_timestamp:
        :param end_timestamp:
        :param one_day_trading_minutes:
        :param kwargs:
        :return:
        """
        if cls.provider_map_recorder:
            print(f"{cls.__name__} registered recorders:{cls.provider_map_recorder}")

            if provider:
                recorder_class = cls.provider_map_recorder[provider]
            else:
                recorder_class = cls.provider_map_recorder[cls.providers[provider_index]]

            # get args for specific recorder class
            from zvtm.contract.recorder import TimeSeriesDataRecorder
            if issubclass(recorder_class, TimeSeriesDataRecorder):
                args = [
                    item
                    for item in inspect.getfullargspec(cls.record_data).args
                    if item not in ("cls", "provider_index", "provider")
                ]
            else:
                args = ["force_update", "sleeping_time"]

            #: just fill the None arg to kw,so we could use the recorder_class default args
            kw = {}
            for arg in args:
                tmp = eval(arg)
                if tmp is not None:
                    kw[arg] = tmp

            # FixedCycleDataRecorder
            from zvtm.contract.recorder import FixedCycleDataRecorder
            if issubclass(recorder_class, FixedCycleDataRecorder):
                #: contract:
                #: 1)use FixedCycleDataRecorder to record the data with IntervalLevel
                #: 2)the table of schema with IntervalLevel format is {entity}_{level}_[adjust_type]_{event}
                table: str = cls.__tablename__
                try:
                    items = table.split("_")
                    if len(items) == 4:
                        adjust_type = items[2]
                        kw["adjust_type"] = adjust_type
                    level = IntervalLevel(items[1])
                except:
                    #: for other schema not with normal format,but need to calculate size for remaining days
                    level = IntervalLevel.LEVEL_1DAY

                kw["level"] = level

                #: add other custom args
                for k in kwargs:
                    kw[k] = kwargs[k]

                r = recorder_class(**kw)
                r.run()
                return
            else:
                r = recorder_class(**kw)
                r.run()
                return
        else:
            print(f"no recorders for {cls.__name__}")


class NormalMixin(Mixin):
    #: the record created time in db
    created_timestamp = Column(DateTime, default=pd.Timestamp.now())
    #: the record updated time in db, some recorder would check it for whether need to refresh
    updated_timestamp = Column(DateTime)


class Entity(Mixin):
    #: 标的类型
    entity_type = Column(String(length=64))
    #: 所属交易所
    exchange = Column(String(length=32))
    #: 编码
    code = Column(String(length=64))
    #: 名字
    name = Column(String(length=128))
    #: 上市日
    list_date = Column(DateTime)
    #: 退市日
    end_date = Column(DateTime)


class TradableEntity(Entity):
    """
    tradable entity
    """

    @classmethod
    def get_trading_dates(cls, start_date=None, end_date=None):
        """
        overwrite it to get the trading dates of the entity

        :param start_date:
        :param end_date:
        :return: list of dates
        """
        return pd.date_range(start_date, end_date, freq="B")

    @classmethod
    def get_trading_intervals(cls):
        """
        overwrite it to get the trading intervals of the entity

        :return: list of time intervals, in format [(start,end)]
        """
        return [("09:30", "11:30"), ("13:00", "15:00")]

    @classmethod
    def in_trading_time(cls, timestamp=None):
        if not timestamp:
            timestamp = now_pd_timestamp()
        else:
            timestamp = pd.Timestamp(timestamp)
        open_time = date_and_time(the_date=timestamp.date(), the_time=cls.get_trading_intervals()[0][0])
        close_time = date_and_time(the_date=timestamp.date(), the_time=cls.get_trading_intervals()[-1][1])
        return open_time < timestamp < close_time

    @classmethod
    def get_close_hour_and_minute(cls):
        hour, minute = cls.get_trading_intervals()[-1][1].split(":")
        return int(hour), int(minute)

    @classmethod
    def get_interval_timestamps(cls, start_date, end_date, level: IntervalLevel):
        """
        generate the timestamps for the level

        :param start_date:
        :param end_date:
        :param level:
        """

        for current_date in cls.get_trading_dates(start_date=start_date, end_date=end_date):
            if level == IntervalLevel.LEVEL_1DAY:
                yield current_date
            elif level == IntervalLevel.LEVEL_1WEEK:
                if current_date.weekday() == 4:
                    yield current_date
            else:
                start_end_list = cls.get_trading_intervals()

                for start_end in start_end_list:
                    start = start_end[0]
                    end = start_end[1]

                    current_timestamp = date_and_time(the_date=current_date, the_time=start)
                    end_timestamp = date_and_time(the_date=current_date, the_time=end)

                    while current_timestamp <= end_timestamp:
                        yield current_timestamp
                        current_timestamp = current_timestamp + timedelta(minutes=level.to_minute())

    @classmethod
    def is_open_timestamp(cls, timestamp):
        timestamp = pd.Timestamp(timestamp)
        return is_same_time(
            timestamp,
            date_and_time(the_date=timestamp.date(), the_time=cls.get_trading_intervals()[0][0]),
        )

    @classmethod
    def is_close_timestamp(cls, timestamp):
        timestamp = pd.Timestamp(timestamp)
        return is_same_time(
            timestamp,
            date_and_time(the_date=timestamp.date(), the_time=cls.get_trading_intervals()[-1][1]),
        )

    @classmethod
    def is_finished_kdata_timestamp(cls, timestamp: pd.Timestamp, level: IntervalLevel):
        """
        :param timestamp: the timestamp could be recorded in kdata of the level
        :type timestamp: pd.Timestamp
        :param level:
        :type level: zvtm.domain.common.IntervalLevel
        :return:
        :rtype: bool
        """
        timestamp = pd.Timestamp(timestamp)

        for t in cls.get_interval_timestamps(timestamp.date(), timestamp.date(), level=level):
            if is_same_time(t, timestamp):
                return True

        return False

    @classmethod
    def could_short(cls):
        """
        whether could be shorted

        :return:
        """
        return False

    @classmethod
    def get_trading_t(cls):
        """
        0 means t+0
        1 means t+1

        :return:
        """
        return 1


class ActorEntity(Entity):
    pass


class NormalEntityMixin(TradableEntity):
    #: the record created time in db
    created_timestamp = Column(DateTime, default=pd.Timestamp.now())
    #: the record updated time in db, some recorder would check it for whether need to refresh
    updated_timestamp = Column(DateTime)


class Portfolio(TradableEntity):
    """
    composition of tradable entities
    """

    @classmethod
    def get_stocks(
        cls,
        code=None,
        codes=None,
        ids=None,
        timestamp=now_pd_timestamp(),
        provider=None,
    ):
        """
        the publishing policy of portfolio positions is different for different types,
        overwrite this function for get the holding stocks in specific date

        :param code: portfolio(etf/block/index...) code
        :param codes: portfolio(etf/block/index...) codes
        :param ids: portfolio(etf/block/index...) ids
        :param timestamp: the date of the holding stocks
        :param provider: the data provider
        :return:
        """
        from zvtm.contract.api import get_schema_by_name
        schema_str = f"{cls.__name__}Stock"
        portfolio_stock = get_schema_by_name(schema_str)
        return portfolio_stock.query_data(provider=provider, code=code, codes=codes, timestamp=timestamp, ids=ids)


#: 组合(Fund,Etf,Index,Block等)和个股(Stock)的关系 应该继承自该类
#: 该基础类可以这样理解:
#: entity为组合本身,其包含了stock这种entity,timestamp为持仓日期,从py的"你知道你在干啥"的哲学出发，不加任何约束
class PortfolioStock(Mixin):
    #: portfolio标的类型
    entity_type = Column(String(length=64))
    #: portfolio所属交易所
    exchange = Column(String(length=32))
    #: portfolio编码
    code = Column(String(length=64))
    #: portfolio名字
    name = Column(String(length=128))

    stock_id = Column(String(length=128))
    stock_code = Column(String(length=64))
    stock_name = Column(String(length=128))


#: 支持时间变化,报告期标的调整
class PortfolioStockHistory(PortfolioStock):
    #: 报告期,season1,half_year,season3,year
    report_period = Column(String(length=32))
    #: 3-31,6-30,9-30,12-31
    report_date = Column(DateTime)

    #: 占净值比例
    proportion = Column(Float)
    #: 持有股票的数量
    shares = Column(Float)
    #: 持有股票的市值
    market_cap = Column(Float)


#: 交易标的和参与者的关系应该继承自该类, meet,遇见,恰如其分的诠释参与者和交易标的的关系
#: 市场就是参与者与交易标的的关系，类的命名规范为{Entity}{relation}{Entity}，entity_id代表"所"为的entity,"受"者entity以具体类别的id命名
#: 比如StockTopTenHolder:TradableMeetActor中entity_id和actor_id,分别代表股票和股东
class TradableMeetActor(Mixin):
    #: tradable code
    code = Column(String(length=64))
    #: tradable name
    name = Column(String(length=128))

    actor_id = Column(String(length=128))
    actor_type = Column(String(length=128))
    actor_code = Column(String(length=64))
    actor_name = Column(String(length=128))


#: 也可以"所"为参与者，"受"为标的
class ActorMeetTradable(Mixin):
    #: actor code
    code = Column(String(length=64))
    #: actor name
    name = Column(String(length=128))

    tradable_id = Column(String(length=128))
    tradable_type = Column(String(length=128))
    tradable_code = Column(String(length=64))
    tradable_name = Column(String(length=128))


# the __all__ is generated
__all__ = [
    "Mixin",
    "NormalMixin",
    "Entity",
    "TradableEntity",
    "ActorEntity",
    "NormalEntityMixin",
    "Portfolio",
    "PortfolioStock",
    "PortfolioStockHistory",
    "TradableMeetActor",
    "ActorMeetTradable",
]
