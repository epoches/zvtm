#保存数据到数据库,每日需要保存的
from zvtm.domain import  Stock1dHfqKdata,StockValuation,Stock1dKdata,Stock,StockSummary

#Stock.record_data(provider='joinquant', sleeping_time=0, day_data=True)
# Stock1dHfqKdata.record_data(provider='joinquant', sleeping_time=0, day_data=True)
#StockValuation.record_data(provider='joinquant', sleeping_time=0)
# Stock1dKdata.record_data(provider='joinquant', sleeping_time=0,code='000156'
#      )#,code='000156',force_update=True,start_timestamp = '2022-01-01', end_timestamp = '2022-03-02'
# StockSummary.record_data(provider='joinquant', sleeping_time=0
#      )
codes = ['600007', '600021', '600027', '600032', '600197', '600217', '600236', '600238', '600284', '600292', '600298', '600371', '600390', '600398', '600428', '600561', '600598', '600697', '600705', '600744', '600757', '600778', '600803', '600815', '600886', '600936', '600956', '601016', '601158', '601330', '601827', '601928', '601949', '601999', '603003', '603017', '603041', '603101', '603333', '603368', '603638', '603733', '603797', '603999', '605003', '605300', '688378', '688567', '000008', '000028', '000060', '000062', '000063', '000070', '000088', '000089', '000099', '000157', '000333', '000401', '000413', '000426', '000429', '000532', '000553', '000563', '000582', '000589', '000597', '000598', '000601', '000612', '000627', '000630', '000635', '000637', '000663', '000680', '000711', '000717', '000731', '000751', '000755', '000758', '000761', '000777', '000778', '000783', '000785', '000789', '000797', '000807', '000823', '000825', '000828', '000829', '000877', '000898', '000905', '000912', '000923', '000927', '000930', '000932', '000933', '000958', '000959', '000960', '000967', '000987', '000990', '001872', '001914', '001965', '002051', '002064', '002075', '002092', '002140', '002150', '002160', '002163', '002167', '002184', '002216', '002226', '002254', '002275', '002289', '002293', '002302', '002307', '002320', '002342', '002348', '002352', '002392', '002415', '002429', '002500', '002508', '002512', '002532', '002588', '002670', '002673', '002677', '002732', '002736', '002760', '002796', '002847', '002867', '002939', '002963', '003013', '003816', '300021', '300197', '300234', '300284', '300288', '300374', '300425', '300461', '300492', '300513', '300517', '300616', '300618', '300621', '300675', '300788', '300791', '300848', '300890', '300894', '300896', '300903', '300977', '300979', '300986', '301038']
Stock1dHfqKdata.record_data(provider='em',codes=codes, sleeping_time=0, day_data=True)