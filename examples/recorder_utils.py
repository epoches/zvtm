# -*- coding: utf-8 -*-
import logging
import time
from typing import Type

from zvtm import zvt_config
from zvtm.contract import Mixin
from zvtm.informer import EmailInformer
#from jqdatasdk import *
from jqdatasdk import auth,get_query_count
logger = logging.getLogger("__name__")


def run_data_recorder(
    domain: Type[Mixin],
    entity_provider=None,
    data_provider=None,
    entity_ids=None,
    retry_times=10,
    sleeping_time=10,
    **recorder_kv,
):
    logger.info(f" record data: {domain.__name__}, entity_provider: {entity_provider}, data_provider: {data_provider}")
    # try:
    #     auth(zvt_config["jq_username"], zvt_config["jq_password"])
    # except Exception as e:
    #     logger.exception("report error:{}".format(e))
    #     quit()
    while retry_times > 0:
        email_action = EmailInformer()


        #spare = get_query_count() , 剩余数据条数:{spare['spare'] }  {zvt_config['jq_username']}剩余数据条数:{spare['spare'] }
        try:
            domain.record_data(
                entity_ids=entity_ids, provider=data_provider, sleeping_time=sleeping_time, **recorder_kv
            )
            if data_provider == 'joinquant':
                auth(zvt_config['jq_username'], zvt_config['jq_password'])
                msg = f"record {domain.__name__} success,数据来源: {data_provider},剩余数据条数:{get_query_count()['spare'] } "
            else:
                msg = f"record {domain.__name__} success,数据来源: {data_provider}"
            logger.info(msg)
            email_action.send_message(zvt_config["email_username"], msg, msg)
            break
        except Exception as e:
            logger.exception("report error:{}".format(e))
            time.sleep(60 * 3)
            retry_times = retry_times - 1
            if retry_times == 0:
                email_action.send_message(
                    zvt_config["email_username"],
                    f"record {domain.__name__} error",
                    f"record {domain.__name__} error: {e}",
                )


if __name__ == "__main__":
    run_data_recorder()
# the __all__ is generated
__all__ = ["run_data_recorder"]
