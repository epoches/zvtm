# -*- coding: utf-8 -*-
from enum import Enum


class IntervalLevel(Enum):
    """
    Repeated fixed time interval, e.g, 5m, 1d.
    """

    #: level tick
    LEVEL_TICK = "tick"
    #: 1 minute
    LEVEL_1MIN = "1m"
    #: 5 minutes
    LEVEL_5MIN = "5m"
    #: 15 minutes
    LEVEL_15MIN = "15m"
    #: 30 minutes
    LEVEL_30MIN = "30m"
    #: 1 hour
    LEVEL_1HOUR = "1h"
    #: 4 hours
    LEVEL_4HOUR = "4h"
    #: 1 day
    LEVEL_1DAY = "1d"
    #: 1 week
    LEVEL_1WEEK = "1wk"
    #: 1 month
    LEVEL_1MON = "1mon"

    def to_pd_freq(self):
        if self == IntervalLevel.LEVEL_1MIN:
            return "1min"
        if self == IntervalLevel.LEVEL_5MIN:
            return "5min"
        if self == IntervalLevel.LEVEL_15MIN:
            return "15min"
        if self == IntervalLevel.LEVEL_30MIN:
            return "30min"
        if self == IntervalLevel.LEVEL_1HOUR:
            return "1H"
        if self == IntervalLevel.LEVEL_4HOUR:
            return "4H"
        if self >= IntervalLevel.LEVEL_1DAY:
            return "1D"

    def floor_timestamp(self, pd_timestamp):
        if self == IntervalLevel.LEVEL_1MIN:
            return pd_timestamp.floor("1min")
        if self == IntervalLevel.LEVEL_5MIN:
            return pd_timestamp.floor("5min")
        if self == IntervalLevel.LEVEL_15MIN:
            return pd_timestamp.floor("15min")
        if self == IntervalLevel.LEVEL_30MIN:
            return pd_timestamp.floor("30min")
        if self == IntervalLevel.LEVEL_1HOUR:
            return pd_timestamp.floor("1h")
        if self == IntervalLevel.LEVEL_4HOUR:
            return pd_timestamp.floor("4h")
        if self == IntervalLevel.LEVEL_1DAY:
            return pd_timestamp.floor("1d")

    def to_minute(self):
        return int(self.to_second() / 60)

    def to_second(self):
        return int(self.to_ms() / 1000)

    def to_ms(self):
        """
        To seconds count in the interval

        :return: seconds count in the interval
        """
        #: we treat tick intervals is 5s, you could change it
        if self == IntervalLevel.LEVEL_TICK:
            return 5 * 1000
        if self == IntervalLevel.LEVEL_1MIN:
            return 60 * 1000
        if self == IntervalLevel.LEVEL_5MIN:
            return 5 * 60 * 1000
        if self == IntervalLevel.LEVEL_15MIN:
            return 15 * 60 * 1000
        if self == IntervalLevel.LEVEL_30MIN:
            return 30 * 60 * 1000
        if self == IntervalLevel.LEVEL_1HOUR:
            return 60 * 60 * 1000
        if self == IntervalLevel.LEVEL_4HOUR:
            return 4 * 60 * 60 * 1000
        if self == IntervalLevel.LEVEL_1DAY:
            return 24 * 60 * 60 * 1000
        if self == IntervalLevel.LEVEL_1WEEK:
            return 7 * 24 * 60 * 60 * 1000
        if self == IntervalLevel.LEVEL_1MON:
            return 31 * 7 * 24 * 60 * 60 * 1000

    def __ge__(self, other):
        if self.__class__ is other.__class__:
            return self.to_ms() >= other.to_ms()
        return NotImplemented

    def __gt__(self, other):

        if self.__class__ is other.__class__:
            return self.to_ms() > other.to_ms()
        return NotImplemented

    def __le__(self, other):
        if self.__class__ is other.__class__:
            return self.to_ms() <= other.to_ms()
        return NotImplemented

    def __lt__(self, other):
        if self.__class__ is other.__class__:
            return self.to_ms() < other.to_ms()
        return NotImplemented


class AdjustType(Enum):
    """
    split-adjusted type for :class:`~.zvt.contract.schema.TradableEntity` quotes

    """

    #: not adjusted
    #: ?????????
    bfq = "bfq"
    #: pre adjusted
    #: ?????????
    qfq = "qfq"
    #: post adjusted
    #: ?????????
    hfq = "hfq"


class ActorType(Enum):
    #: ??????
    individual = "individual"
    #: ????????????
    raised_fund = "raised_fund"
    #: ??????
    social_security = "social_security"
    #: ??????
    insurance = "insurance"
    #: ??????
    qfii = "qfii"
    #: ??????
    trust = "trust"
    #: ??????
    broker = "broker"
    #: ??????
    private_equity = "private_equity"
    #: ??????(??????????????????)
    corporation = "corporation"


class TradableType(Enum):
    #: A???(??????)
    #: China stock
    stock = "stock"
    #: A?????????(??????)
    #: China index
    index = "index"
    #: A?????????(??????)
    #: China stock block
    block = "block"
    #: ??????
    #: USA stock
    stockus = "stockus"
    #: ????????????
    #: USA index
    indexus = "indexus"
    #: ??????
    #: Hongkong Stock
    stockhk = "stockhk"
    #: ??????(??????)
    #: China future
    future = "future"
    #: ????????????
    #: Cryptocurrency
    coin = "coin"
    #: ??????(??????)
    #: China option
    option = "option"
    #: ??????(??????)
    #: China fund
    fund = "fund"
    #: ????????????
    #: currency exchange rate
    currency = "currency"


class Exchange(Enum):
    #: ???????????????
    sh = "sh"
    #: ???????????????
    sz = "sz"

    #: ?????????????????????????????? ??????
    cn = "cn"
    #: ?????????????????????????????? ??????
    us = "us"

    #: ????????????
    nasdaq = "nasdaq"

    #: ?????????
    nyse = "nyse"

    #: ?????????
    hk = "hk"

    #: ????????????
    binance = "binance"
    huobipro = "huobipro"

    #: ?????????????????????
    shfe = "shfe"
    #: ?????????????????????
    dce = "dce"
    #: ?????????????????????
    czce = "czce"
    #: ???????????????????????????
    cffex = "cffex"
    #: ??????????????????????????????
    ine = "ine"

    #: ???????????????(??????)
    #: currency exchange(virtual)
    forex = "forex"
    #: ??????????????????


tradable_type_map_exchanges = {
    TradableType.block: [Exchange.cn],
    TradableType.index: [Exchange.sh, Exchange.sz],
    TradableType.stock: [Exchange.sh, Exchange.sz],
    TradableType.stockhk: [Exchange.hk],
    TradableType.stockus: [Exchange.nasdaq, Exchange.nyse],
    TradableType.indexus: [Exchange.us],
    TradableType.future: [Exchange.shfe, Exchange.dce, Exchange.czce, Exchange.cffex, Exchange.ine],
    TradableType.coin: [Exchange.binance, Exchange.huobipro],
    TradableType.currency: [Exchange.forex],
}


def get_entity_exchanges(entity_type):
    entity_type = TradableType(entity_type)
    return tradable_type_map_exchanges.get(entity_type)


from .context import zvt_context

zvt_context = zvt_context

# the __all__ is generated
__all__ = [
    "IntervalLevel",
    "AdjustType",
    "ActorType",
    "TradableType",
    "Exchange",
    "tradable_type_map_exchanges",
    "get_entity_exchanges",
    "zvt_context",
]

# __init__.py structure:
# common code of the package
# export interface in __all__ which contains __all__ of its sub modules

# import all from submodule schema
from .schema import *
from .schema import __all__ as _schema_all

__all__ += _schema_all
