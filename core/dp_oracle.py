# encoding: utf-8
# author TurboChang

import os
import sys
import cx_Oracle
import csv
base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(base_dir)
from assets.conf_case import *

class OracleDB:
    ALIAS = 'ORACLE'

    def __init__(self, table_name):
        self.parent_path = os.getcwd()
        self.col_files = self.parent_path + "/save/col_name/tab_col"
        self.keys_data = self.parent_path + "/save/keys/save_keys"
        self.col_name = self.__read_colname()
        self.read_col = self.col_name.read()
        self.host = db_info[0]
        self.port = db_info[1]
        self.user = db_info[2]
        self.password = db_info[3]
        self.database = db_info[4]
        self.table_name = table_name
        self.db = self.__connect()
        self.datatype = self.get_data_type()
        self.csv_file = self.parent_path + "/save/{0}.csv".format(self.table_name)

    def __del__(self):
        try:
            self.db.close()
            self.col_name.close()
        except cx_Oracle.Error as e:
            print(e)

    def __read_colname(self):
        return open(self.col_files, "r")

    def __connect(self):
        # print('连接Oracle数据库 host: {0}, port: {1}, user: {2}, passwd: {3}, db: {4}'.format(
            # self.host, self.port, self.user, self.password, self.database))
        db = cx_Oracle.connect('{0}/{1}@{2}:{3}/{4}'.format(
            self.user, self.password, self.host, self.port, self.database))
        db.ping()
        return db

    def cut_list(self, lists, cut_len):
        """
        将列表拆分为指定长度的多个列表
        :param lists: 初始列表
        :param cut_len: 每个列表的长度
        :return: 一个二维数组 [[x,x],[x,x]]
        """
        res_data = []
        if len(lists) > cut_len:
            for i in range(int(len(lists) / cut_len)):
                cut_a = lists[cut_len * i:cut_len * (i + 1)]
                res_data.append(cut_a)
            last_data = lists[int(len(lists) / cut_len) * cut_len:]
            if last_data:
                res_data.append(last_data)
        else:
            res_data.append(lists)
        return res_data

    def __execute(self, sql):
        cursor = self.db.cursor()
        cursor.execute(sql)
        results = cursor.fetchall()
        return results

    def get_pk_col(self):
        sql = primary_key.format(self.table_name)
        col_name = self.__execute(sql)
        col_name1 = [",".join(x) for x in col_name]
        return col_name1

    def get_data_type(self):
        # 需要把 self.read_col 中的的date类型做转变
        sql = col_data_type.format(self.table_name)
        datatype = self.__execute(sql)
        for col in datatype:
            col_name = col[0]
            data_type = col[1]
            if data_type == "DATE":
                return col_name

    def query(self):
        primary_key = open(self.keys_data, "r")
        keys = primary_key.read()
        bind_values = list(map(str, keys.split("|")))
        values = ",".join(bind_values)
        # bind_names = [":" + str(i + 1) for i in range(len(bind_values))]
        # print(bind_names)
        cols = ",".join(self.get_pk_col())
        sql_text = "select {0} from {1} where ("+ cols +", COL1) in (%s) order by {2}, COL1" % values
        query_cols = self.read_col.replace(self.datatype, "to_char({0},'yy-mm-dd hh24:mi:ss')".format(self.datatype))
        sql = sql_text.format(query_cols, self.table_name, cols)
        cursor = self.db.cursor()
        cursor.prepare(sql)
        cursor.execute(sql)
        results = cursor.fetchall()
        cursor.close()
        primary_key.close()

        # Persistence Oracle to CSV
        to_csv = open(self.csv_file, "w", encoding="utf-8")
        writer = csv.writer(to_csv)
        print(self.read_col)
        writer.writerow(self.read_col)  # write csv title from data table columns
        for row in results:
            writer.writerow(row)
        to_csv.close()

if __name__ == '__main__':
    f = OracleDB("T1")
    g = f.query()
    # g = f.get_data_type()
    print(g)