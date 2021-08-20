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

class StoreKafka:

    def __init__(self, primary_key):
        f = KafkaConsumer(topic, begin_time, end_time)
        self.message = f.consume_kafka()
        self.prikey = primary_key
        self.dict_list = []
        self.keys_list = []

    def merge_data(self, brfore_list, after_list):
        if isinstance(brfore_list, dict) and isinstance(after_list, dict):
            new_dict = {}
            d2_keys = list(after_list.keys())
            for d1k in brfore_list.keys():
                if d1k in d2_keys:  # d1,d2都有,去往深层比对
                    d2_keys.remove(d1k)
                    new_dict[d1k] = self.merge_data(brfore_list.get(d1k), after_list.get(d1k))
                else:
                    new_dict[d1k] = brfore_list.get(d1k)  # d1有d2没有的key
            for d2k in d2_keys:  # d2有d1没有的key
                new_dict[d2k] = after_list.get(d2k)
            return new_dict
        else:
            return after_list

    def store_data(self):
        for msg in self.message:
            for key, value in msg.items():
                if key == "msg_val":
                    d = json.loads(value)
                    if d['OP'] == "I":
                        data = d['DATA']
                        self.keys_list.append(list(data.keys()))
                        self.dict_list.append(data)
                    elif d['OP'] == "U":
                        data = d['DATA']
                        where = d['WHERE']
                        results = self.merge_data(where, data)
                        self.dict_list.append(results)
                    elif d['OP'] == "D":
                        where = d['WHERE']
                        self.dict_list.append(where)

        keys = self.keys_list[0]
        keys = ",".join(str(x) for x in keys)
        print(keys)

        dict_diff = pd.DataFrame(self.dict_list)
        merge_diff = dict_diff.drop_duplicates(subset=[self.prikey], keep=False)
        distinct_list = []

        for _, item in merge_diff.iterrows():
            distinct_list.append(item.dropna().to_dict())

        for dict in distinct_list:
            datas = ",".join(list(dict.values()))
            print(datas)

if __name__ == '__main__':
    f = StoreKafka("ID")
    f.store_data()

