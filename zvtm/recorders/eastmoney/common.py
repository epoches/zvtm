# -*- coding: utf-8 -*-
import logging

import requests

from zvtm.contract.api import get_data_count, get_data
from zvtm.contract.recorder import TimestampsDataRecorder, TimeSeriesDataRecorder
from zvtm.domain import CompanyType
from zvtm.domain.meta.stock_meta import Stock, StockDetail
from zvtm.utils.time_utils import to_pd_timestamp

logger = logging.getLogger(__name__)


class ApiWrapper(object):
    def request(self, url=None, method="post", param=None, path_fields=None):
        raise NotImplementedError


def get_fc(security_item):
    if security_item.exchange == "sh":
        fc = "{}01".format(security_item.code)
    if security_item.exchange == "sz":
        fc = "{}02".format(security_item.code)

    return fc


def get_company_type(stock_domain: StockDetail):
    if stock_domain.industries is not None:
        industries = stock_domain.industries.split(",")
    else:
        industries = ""
    if ("银行" in industries) or ("信托" in industries):
        return CompanyType.yinhang
    if "保险" in industries:
        return CompanyType.baoxian
    if "证券" in industries:
        return CompanyType.quanshang
    return CompanyType.qiye

import gzip
import json
def is_valid_json(json_str):
    try:
        # 尝试解析字符串为JSON
        parsed_json = json.loads(json_str)
        return True, parsed_json  # 返回一个元组，包含布尔值和解析后的JSON对象
    except json.JSONDecodeError:
        # 如果抛出JSONDecodeError异常，说明不是有效的JSON
        return False, None

def decode_text(encoded_text, encodings):
    for encoding in encodings:
        try:
            decoded_text = encoded_text.decode(encoding)
            print(f"Trying {encoding}...")
            print(decoded_text)
            return decoded_text
        except UnicodeDecodeError:
            continue
    return None

encodings_to_try = ['utf-8', 'gbk']

def company_type_flag(security_item):
    try:
        company_type = get_company_type(security_item)

        if company_type == CompanyType.qiye:
            return "4"
        if company_type == CompanyType.quanshang:
            return "1"
        if company_type == CompanyType.baoxian:
            return "2"
        if company_type == CompanyType.yinhang:
            return "3"
    except Exception as e:
        logger.warning(e)

    param = {"color": "w", "fc": get_fc(security_item)}

    resp = requests.post("https://emh5.eastmoney.com/api/CaiWuFenXi/GetCompanyType", json=param)

    # ct = resp.json().get("Result").get("CompanyType")
    # try:
    # 尝试解析JSON
    content = decode_text(resp.content, encodings_to_try)
    # 检查字符串中的 Status 字段是否为 '0'
    # status_str = '"Status":0'
    # print(content)
    if content is not None:#status_str in content:#isinstance(resp.content, dict) and resp.text and is_valid_json(resp.text):#isinstance(resp.json()["Result"], dict):
        # print(resp.content)
        # data = resp.json()
        data = json.loads(content).get("Result")
        if data is not None:
            ct = data["Result"].get("CompanyType")
        else:
            ct = 4
    else:
        decompressed_data = gzip.decompress(resp.content)
        # 假设解压缩后的数据是UTF-8编码的
        decoded_text = decompressed_data.decode('utf-8')
        # 尝试将解码后的内容解析为字典
        json_dict = json.loads(decoded_text)
        print("resp.content 被解码并解析为字典:{}", json_dict)
        logger.warning("{} not catching json dict:{}".format(security_item, json_dict))
        ct = 4  # 或者设定其他默认行为
    # except json.JSONDecodeError as e:
    #     # JSON解析错误，记录异常
    #     logging.error("JSONDecodeError: {}".format(e))
    #     result = None  # 或者进行其他错误处理
    # logger.warning("{} not catching company type:{}".format(security_item, ct))

    return ct



def call_eastmoney_api(url=None, method="post", param=None, path_fields=None):
    if method == "post":
        resp = requests.post(url, json=param)
    origin_result = {}
    # try:
    # 尝试解析JSON
    content = decode_text(resp.content, encodings_to_try)
    # status_str = '"Status":0'
    # print(content)
    if content is not None:#status_str in content:#isinstance(resp.content, dict) and resp.text and is_valid_json(resp.text):#isinstance(resp.json()["Result"], dict):
        # print(resp.content)
        # data = resp.json()
        # origin_result = data.get("Result")
        # # origin_result = decode_text(origin_result, encodings_to_try)
        # # print(origin_result)
        origin_result = json.loads(content).get("Result")
    else:
        decompressed_data = gzip.decompress(resp.content)
        # 假设解压缩后的数据是UTF-8编码的
        decoded_text = decompressed_data.decode('utf-8')
        # 尝试将解码后的内容解析为字典
        json_dict = json.loads(decoded_text)
        print("resp.content 被解码并解析为字典:{}",json_dict)
        logger.exception("code:{},content:{},{}".format(resp.status_code, resp.text,json_dict))
        return None
    # except json.JSONDecodeError as e:
    #     # JSON解析错误，记录异常
    #     logging.error("JSONDecodeError: {}".format(e))
    # resp.encoding = "utf8"
    # #import gzip
    # try:
    #     #gzip.decompress(resp.json().get("Result")).decode("utf-8")
    #     origin_result = resp.json().get("Result")
    # except Exception as e:
    #     logger.exception("code:{},content:{}".format(resp.status_code, resp.text))
    #     #
    #     raise e

    if origin_result is not None and path_fields:
        the_data = get_from_path_fields(origin_result, path_fields)
        if not the_data:
            logger.warning(
                "url:{},param:{},origin_result:{},could not get data for nested_fields:{}".format(
                    url, param, origin_result, path_fields
                )
            )
        return the_data

    return origin_result


def get_from_path_fields(the_json, path_fields):
    the_data = the_json.get(path_fields[0])
    if the_data:
        for field in path_fields[1:]:
            the_data = the_data.get(field)
            if not the_data:
                return None
    return the_data


class EastmoneyApiWrapper(ApiWrapper):
    def request(self, url=None, method="post", param=None, path_fields=None):
        return call_eastmoney_api(url=url, method=method, param=param, path_fields=path_fields)


class BaseEastmoneyRecorder(object):
    request_method = "post"
    path_fields = None
    api_wrapper = EastmoneyApiWrapper()

    def generate_request_param(self, security_item, start, end, size, timestamp):
        raise NotImplementedError

    def record(self, entity_item, start, end, size, timestamps):
        if timestamps:
            original_list = []
            for the_timestamp in timestamps:
                param = self.generate_request_param(entity_item, start, end, size, the_timestamp)
                tmp_list = self.api_wrapper.request(
                    url=self.url, param=param, method=self.request_method, path_fields=self.path_fields
                )
                self.logger.info(
                    "record {} for entity_id:{},timestamp:{}".format(self.data_schema, entity_item.id, the_timestamp)
                )
                # fill timestamp field
                for tmp in tmp_list:
                    tmp[self.get_evaluated_time_field()] = the_timestamp
                original_list += tmp_list
                if len(original_list) == 50:
                    break
            return original_list

        else:
            param = self.generate_request_param(entity_item, start, end, size, None)
            return self.api_wrapper.request(
                url=self.url, param=param, method=self.request_method, path_fields=self.path_fields
            )


class EastmoneyTimestampsDataRecorder(BaseEastmoneyRecorder, TimestampsDataRecorder):
    entity_provider = "eastmoney"
    entity_schema = StockDetail

    provider = "eastmoney"

    timestamps_fetching_url = None
    timestamp_list_path_fields = None
    timestamp_path_fields = None

    def init_timestamps(self, entity):
        param = {"color": "w", "fc": get_fc(entity)}

        timestamp_json_list = call_eastmoney_api(
            url=self.timestamps_fetching_url, path_fields=self.timestamp_list_path_fields, param=param
        )

        if self.timestamp_path_fields and timestamp_json_list:
            timestamps = [get_from_path_fields(data, self.timestamp_path_fields) for data in timestamp_json_list]
            return [to_pd_timestamp(t) for t in timestamps]
        return []


class EastmoneyPageabeDataRecorder(BaseEastmoneyRecorder, TimeSeriesDataRecorder):
    entity_provider = "eastmoney"
    entity_schema = StockDetail

    provider = 'eastmoney'

    page_url = None

    def get_remote_count(self, security_item):
        param = {"color": "w", "fc": get_fc(security_item), "pageNum": 1, "pageSize": 1}
        return call_eastmoney_api(self.page_url, param=param, path_fields=["TotalCount"])

    def evaluate_start_end_size_timestamps(self, entity):
        remote_count = self.get_remote_count(entity)

        if remote_count == 0:
            return None, None, 0, None

        # get local count
        local_count = get_data_count(
            data_schema=self.data_schema, session=self.session, filters=[self.data_schema.entity_id == entity.id]
        )
        # FIXME:the > case
        if local_count >= remote_count:
            return None, None, 0, None

        return None, None, remote_count - local_count, None

    def generate_request_param(self, security_item, start, end, size, timestamp):
        return {
            "color": "w",
            "fc": get_fc(security_item),
            "pageNum": 1,
            # just get more for some fixed data
            "pageSize": size + 10,
        }


class EastmoneyMoreDataRecorder(BaseEastmoneyRecorder, TimeSeriesDataRecorder):
    entity_provider = "eastmoney"
    entity_schema = StockDetail

    provider = "eastmoney"

    def get_remote_latest_record(self, security_item):
        param = {"color": "w", "fc": get_fc(security_item), "pageNum": 1, "pageSize": 1}
        results = call_eastmoney_api(self.url, param=param, path_fields=self.path_fields)
        _, result = self.generate_domain(security_item, results[0])
        return result

    def evaluate_start_end_size_timestamps(self, entity):
        # get latest record
        latest_record = get_data(
            entity_id=entity.id,
            provider=self.provider,
            data_schema=self.data_schema,
            order=self.data_schema.timestamp.desc(),
            limit=1,
            return_type="domain",
            session=self.session,
        )
        if latest_record:
            remote_record = self.get_remote_latest_record(entity)
            if not remote_record or (latest_record[0].id == remote_record.id):
                return None, None, 0, None
            else:
                return None, None, 10, None

        return None, None, 1000, None

    def generate_request_param(self, security_item, start, end, size, timestamp):
        return {"color": "w", "fc": get_fc(security_item), "pageNum": 1, "pageSize": size}


# the __all__ is generated
__all__ = [
    "ApiWrapper",
    "get_fc",
    "get_company_type",
    "company_type_flag",
    "call_eastmoney_api",
    "get_from_path_fields",
    "EastmoneyApiWrapper",
    "BaseEastmoneyRecorder",
    "EastmoneyTimestampsDataRecorder",
    "EastmoneyPageabeDataRecorder",
    "EastmoneyMoreDataRecorder",
]
