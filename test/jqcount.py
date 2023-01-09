from zvtm import zvt_config
from jqdatasdk import auth,get_query_count


auth(zvt_config['jq_username'], zvt_config['jq_password'])
print(get_query_count())
spare = get_query_count()
print(spare['spare'])