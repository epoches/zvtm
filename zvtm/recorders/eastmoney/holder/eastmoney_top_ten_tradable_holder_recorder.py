# -*- coding: utf-8 -*-
from zvtm.domain import TopTenTradableHolder
from zvtm.recorders.eastmoney.holder.eastmoney_top_ten_holder_recorder import TopTenHolderRecorder


class TopTenTradableHolderRecorder(TopTenHolderRecorder):
    provider = "eastmoney"
    data_schema = TopTenTradableHolder

    url = "https://emh5.eastmoney.com/api/GuBenGuDong/GetShiDaLiuTongGuDong"
    path_fields = ["ShiDaLiuTongGuDongList"]
    timestamps_fetching_url = "https://emh5.eastmoney.com/api/GuBenGuDong/GetFirstRequest2Data"
    timestamp_list_path_fields = ["SDLTGDBGQ", "ShiDaLiuTongGuDongBaoGaoQiList"]
    timestamp_path_fields = ["BaoGaoQi"]


if __name__ == "__main__":
    # init_log('top_ten_tradable_holder.log')

    TopTenTradableHolderRecorder(codes=["002572"]).run()
# the __all__ is generated
__all__ = ["TopTenTradableHolderRecorder"]
