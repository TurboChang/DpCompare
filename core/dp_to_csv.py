# encoding: utf-8
# author TurboChang

import os
import sys
import json
import csv
import pandas as pd
from core.dp_consume import KafkaConsumer
from datetime import datetime, timedelta
base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(base_dir)
from assets.conf_case import *

class StoreKafka:

    def __init__(self, primary_key:list):
        self.begin = self.__begin_2_utc(begin_time)
        self.end = self.__end_2_utc(end_time)
        f = KafkaConsumer(topic, self.begin, self.end)
        self.message = f.consume_kafka()
        self.prikey = primary_key
        self.dict_list = []
        self.keys_list = []
        self.parent_path = os.getcwd()
        self.col_files = self.parent_path + "/save/col_name/tab_col"
        self.csv_file = self.parent_path + "/save/{0}.csv".format(topic)
        self.prikeys_list = []

    def __begin_2_utc(self, pktime_str: str) -> str:
        now_time = datetime.strptime(pktime_str, "%Y-%m-%d %H:%M:%S")
        utc_time = now_time - timedelta(hours=0.25) - timedelta(hours=range_time)
        utc = str(datetime.strptime(str(utc_time), "%Y-%m-%d %H:%M:%S"))
        return utc

    def __end_2_utc(self, pktime_str: str) -> str:
        now_time = datetime.strptime(pktime_str, "%Y-%m-%d %H:%M:%S")
        utc_time = now_time - timedelta(hours=0.25) + timedelta(hours=range_time)
        utc = str(datetime.strptime(str(utc_time), "%Y-%m-%d %H:%M:%S"))
        return utc

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

        # Persistence Kafka to CSV
        keys = self.keys_list[0]
        print(keys)
        to_csv = open(self.csv_file, "w", encoding="utf-8")
        writer = csv.writer(to_csv)
        writer.writerow(keys)   # write csv title from data table columns
        keys = ",".join(str(x) for x in keys)
        wf = open(self.col_files, "w")
        wf.write(keys)
        dict_diff = pd.DataFrame(self.dict_list)
        merge = dict_diff.sort_values(self.prikey[0]).drop_duplicates(subset=self.prikey, keep=False)
        for _, item in merge.iterrows():
            data_dict = item.dropna().to_dict()
            values = (",".join(["'" + data_dict[id] + "'" for id in self.prikey]))    # get primary key column values
            self.prikeys_list.append(values)
            datas = list(data_dict.values())
            writer.writerow(datas)
        to_csv.close()
        wf.close()

        # Oracle PK Column Values
        keys_file = self.parent_path + "/save/keys/save_keys"
        wf = open(keys_file, "w")
        res = [(ele,) for ele in self.prikeys_list]
        results = ["("+tups[0]+")" for tups in res]
        d = "|".join(results)
        wf.write(d)
        wf.close()
