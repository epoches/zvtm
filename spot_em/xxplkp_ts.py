import requests
import pandas as pd
from tqdm import tqdm


def get_szse_supervision_check(start_date: str = "2022-01-01", end_date: str = "2022-12-31") -> pd.DataFrame:
    """
    获取深圳证券交易所监管公告数据
    https://www.szse.cn/disclosure/supervision/check/index.html

    :param start_date: 开始日期, 格式为"YYYY-MM-DD"
    :param end_date: 结束日期, 格式为"YYYY-MM-DD"
    :return: 监管公告数据
    :rtype: pandas.DataFrame
    """
    url = "https://www.szse.cn/api/report/ShowReport"
    params = {
        "SHOWTYPE": "JSON",
        "CATALOGID": "1837_xxpl",
        "TABKEY": "tab1",
        "PAGENO": "1",
        "PAGESIZE": "50",
        "STARTDATE": start_date,
        "ENDDATE": end_date,
    }

    # 获取总页数
    r = requests.get(url, params=params)
    data_json = r.json()
    total_pages = data_json["data"]["pages"]

    # 获取所有页数据
    big_df = pd.DataFrame()
    tqdm_obj = tqdm(range(1, total_pages + 1), leave=False)
    for page in tqdm_obj:
        params["PAGENO"] = str(page)
        r = requests.get(url, params=params)
        data_json = r.json()
        temp_df = pd.DataFrame(data_json["data"]["data"])
        big_df = pd.concat([big_df, temp_df], ignore_index=True)

    # 处理列名
    big_df.columns = [
        "序号",
        "公告标题",
        "公告时间",
        "公告类型",
        "公告内容",
        "公告链接",
    ]
    big_df["公告时间"] = pd.to_datetime(big_df["公告时间"])
    big_df["公告类型"] = big_df["公告类型"].str.strip()
    return big_df

print(get_szse_supervision_check)