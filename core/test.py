# encoding: utf-8
# author TurboChang

import os
import sys
import json
from core.consume import KafkaConsumer
base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(base_dir)
from assets.conf_case import *

f = KafkaConsumer(topic, begin_time, end_time)
msg = f.consume_kafka()

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

insert_list = []
update_list = []
keys_list = []
for m in msg:
    for key, value in m.items():
        if key == "msg_val":
            d = json.loads(value)
            if d['OP'] == "I":
                data = d['DATA']
                data['SCNTIME'] = d['SCNTIME']
                keys_list.append(list(data.keys()))
                insert_list.append(list(data.values()))
            if d['OP'] == "U":
                data = d['DATA']
                where = d['WHERE']
                where['SCNTIME'] = d['SCNTIME']
                results = merge_data(where, data)
                update_list.append(list(results.values()))
            if d['OP'] == "D":
                continue

keys = keys_list[0]
keys = ",".join(str(x) for x in keys)
print(keys)
for row in insert_list:
    data = ",".join(str(x) for x in row)
    print(data)
for row in update_list:
    data = ",".join(str(x) for x in row)
    print(data)


