from zvtm.domain import HolderTrading,ManagerTrading,MarginTrading,DragonAndTiger,BigDealTrading

# HolderTrading.record_data(provider='eastmoney', sleeping_time=0,code='300001')
# sqlalchemy.exc.IntegrityError: (pymysql.err.IntegrityError) (1062, "Duplicate entry 'stock_sz_300001_2014-09-30_HELMUT BRUNO REBSTOCK' for key 'PRIMARY'")
# [SQL: INSERT INTO holder_trading (id, entity_id, timestamp, provider, code, holder_name, volume, change_pct, holding_pct) VALUES (%(id)s, %(entity_id)s, %(timestamp)s, %(provider)s, %(code)s, %(holder_name)s, %(volume)s, %(change_pct)s, %(holding_pct)s)]
# [parameters: ({'id': 'stock_sz_300001_2022-12-08_青岛德锐投资有限公司', 'entity_id': 'stock_sz_300001', 'timestamp': Timestamp('2022-12-08 00:00:00'), 'provider': None, 'code': '300001', 'holder_name': '青岛德锐投资有限公司', 'volume': -55000000.0, 'change_pct': -0.1416, 'holding_pct': 0.3203}, {'id': 'stock_sz_300001_2022-11-18_青岛特锐德电气股份有限公司第三期员工持股计划', 'entity_id': 'stock_sz_300001', 'timestamp': Timestamp('2022-11-18 00:00:00'), 'provider': None, 'code': '300001', 'holder_name': '青岛特锐德电气股份有限公司第三期员工持股计划', 'volume': -9285000.0, 'change_pct': -1.0, 'holding_pct': None}, {'id': 'stock_sz_300001_2022-09-30_金算盘虎虎生威1号私募证券投资基金', 'entity_id': 'stock_sz_300001', 'timestamp': Timestamp('2022-09-30 00:00:00'), 'provider': None, 'code': '300001', 'holder_name': '金算盘虎虎生威1号私募证券投资基金', 'volume': -40000.0, 'change_pct': -0.0052, 'holding_pct': 0.0074}, {'id': 'stock_sz_300001_2022-09-30_全国社保基金503组合', 'entity_id': 'stock_sz_300001', 'timestamp': Timestamp('2022-09-30 00:00:00'), 'provider': None, 'code': '300001', 'holder_name': '全国社保基金503组合', 'volume': 1000000.0, 'change_pct': 0.0769, 'holding_pct': 0.0135}, {'id': 'stock_sz_300001_2022-09-30_香港中央结算有限公司', 'entity_id': 'stock_sz_300001', 'timestamp': Timestamp('2022-09-30 00:00:00'), 'provider': None, 'code': '300001', 'holder_name': '香港中央结算有限公司', 'volume': -3724000.0, 'change_pct': -0.1839, 'holding_pct': 0.0159}, {'id': 'stock_sz_300001_2022-06-30_全国社保基金503组合', 'entity_id': 'stock_sz_300001', 'timestamp': Timestamp('2022-06-30 00:00:00'), 'provider': None, 'code': '300001', 'holder_name': '全国社保基金503组合', 'volume': -3000000.0, 'change_pct': -0.1875, 'holding_pct': 0.0125}, {'id': 'stock_sz_300001_2022-06-30_香港中央结算有限公司', 'entity_id': 'stock_sz_300001', 'timestamp': Timestamp('2022-06-30 00:00:00'), 'provider': None, 'code': '300001', 'holder_name': '香港中央结算有限公司', 'volume': -18430000.0, 'change_pct': -0.4764, 'holding_pct': 0.0195}, {'id': 'stock_sz_300001_2022-03-31_东方新能源汽车主题混合型证券投资基金', 'entity_id': 'stock_sz_300001', 'timestamp': Timestamp('2022-03-31 00:00:00'), 'provider': None, 'code': '300001', 'holder_name': '东方新能源汽车主题混合型证券投资基金', 'volume': -61340.0, 'change_pct': -0.0047, 'holding_pct': 0.0124}  ... displaying 10 of 209 total bound parameter sets ...  {'id': 'stock_sz_300001_2010-10-29_张延森', 'entity_id': 'stock_sz_300001', 'timestamp': Timestamp('2010-10-29 00:00:00'), 'provider': None, 'code': '300001', 'holder_name': '张延森', 'volume': -500.0, 'change_pct': None, 'holding_pct': None}, {'id': 'stock_sz_300001_2010-06-30_李镇桂', 'entity_id': 'stock_sz_300001', 'timestamp': Timestamp('2010-06-30 00:00:00'), 'provider': None, 'code': '300001', 'holder_name': '李镇桂', 'volume': 200300.0, 'change_pct': 1.3721, 'holding_pct': 0.0026})]
# (Background on this error at: http://sqlalche.me/e/14/gkpj)
# MarginTrading.record_data(provider='em', sleeping_time=0)
DragonAndTiger.record_data(provider='em', sleeping_time=0)
# BigDealTrading.record_data(provider='em', sleeping_time=0)