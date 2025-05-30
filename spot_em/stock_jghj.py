# 深交所监管函件
import pandas as pd
import requests
import re
import random
from datetime import datetime


def get_szse_regulatory_letters(stock_code):
    """
    获取深交所上市公司监管函件数据

    参数:
    stock_code -- 股票代码，如 '002656'

    返回:
    DataFrame包含以下列:
        - 公司代码
        - 公司简称
        - 发函日期
        - 函件类别
        - 函件链接
        - 公司回复链接
    """
    # 构造请求URL
    base_url = "https://www.szse.cn/api/report/ShowReport/data"
    params = {
        "SHOWTYPE": "JSON",
        "CATALOGID": "main_wxhj",
        "TABKEY": "tab2",  # 主板市场
        "txtZqdm": stock_code,
        "random": random.random()  # 添加随机参数避免缓存
    }

    # 设置请求头
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Referer": "https://www.szse.cn/disclosure/supervision/letter/index.html",
        "Accept": "application/json, text/javascript, */*; q=0.01"
    }

    try:
        # 发送请求
        response = requests.get(base_url, params=params, headers=headers, timeout=10)
        response.raise_for_status()  # 检查请求是否成功

        # 解析JSON数据
        json_data = response.json()

        # 提取主板和创业板数据
        mainboard_data = json_data[0]['data']
        gem_data = json_data[1]['data'] if len(json_data) > 1 else []
        all_data = mainboard_data + gem_data

        # 如果没有数据
        if not all_data:
            return pd.DataFrame(columns=['公司代码', '公司简称', '发函日期', '函件类别', '函件链接', '公司回复链接'])

        # 处理数据
        processed = []
        for item in all_data:
            record = {
                '公司代码': item['gsdm'],
                '公司简称': item['gsjc'],
                '发函日期': item['fhrq'],
                '函件类别': item['hjlb']
            }

            # 提取函件链接
            ck_match = re.search(r"encode-open='([^']*)'", item['ck'])
            record['函件链接'] = f"https://www.szse.cn{ck_match.group(1)}" if ck_match else None

            # 提取回复链接
            if item['hfck']:
                hf_match = re.search(r"encode-open='([^']*)'", item['hfck'])
                record['公司回复链接'] = f"https://www.szse.cn{hf_match.group(1)}" if hf_match else None
            else:
                record['公司回复链接'] = None

            processed.append(record)

        # 创建DataFrame
        df = pd.DataFrame(processed)

        # 转换日期格式并排序
        df['发函日期'] = pd.to_datetime(df['发函日期'])
        df = df.sort_values('发函日期', ascending=False).reset_index(drop=True)

        return df

    except requests.exceptions.RequestException as e:
        print(f"请求失败: {e}")
        return pd.DataFrame()
    except (KeyError, IndexError, TypeError) as e:
        print(f"数据解析错误: {e}")
        return pd.DataFrame()


# 使用示例
if __name__ == "__main__":
    df = get_szse_regulatory_letters("002656")
    print(f"获取到 {len(df)} 条监管函件记录")
    print(df.head())