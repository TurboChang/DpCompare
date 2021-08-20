# encoding: utf-8
# author TurboChang

import os
import sys
import json
import pandas as pd
from core.consume import KafkaConsumer
base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(base_dir)
from assets.conf_case import *

f = KafkaConsumer(topic, begin_time, end_time)
message = f.consume_kafka()


def merge_data(brfore, after):
    if isinstance(brfore, dict) and isinstance(after, dict):
        new_dict = {}
        d2_keys = list(after.keys())
        for d1k in brfore.keys():
            if d1k in d2_keys:  # d1,d2都有,去往深层比对
                d2_keys.remove(d1k)
                new_dict[d1k] = merge_data(brfore.get(d1k), after.get(d1k))
            else:
                new_dict[d1k] = brfore.get(d1k)  # d1有d2没有的key
        for d2k in d2_keys:  # d2有d1没有的key
            new_dict[d2k] = after.get(d2k)
        return new_dict
    else:
        return after


dict_list = []
keys_list = []

for msg in message:
    for key, value in msg.items():
        if key == "msg_val":
            d = json.loads(value)
            if d['OP'] == "I":
                data = d['DATA']
                keys_list.append(list(data.keys()))
                dict_list.append(data)
            elif d['OP'] == "U":
                data = d['DATA']
                where = d['WHERE']
                results = merge_data(where, data)
                dict_list.append(results)
            elif d['OP'] == "D":
                where = d['WHERE']
                dict_list.append(where)

keys = keys_list[0]
keys = ",".join(str(x) for x in keys)
print(keys)

dict_diff = pd.DataFrame(dict_list)
merge_diff = dict_diff.drop_duplicates(subset=['ID'], keep=False)
distinct_list = []

for _, item in merge_diff.iterrows():
    distinct_list.append(item.dropna().to_dict())

for dict in distinct_list:
    print(dict)
