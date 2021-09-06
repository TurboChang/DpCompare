# encoding: utf-8
# author TurboChang

import os
import re
import sys
import csv
import cx_Oracle
base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(base_dir)
from assets.conf_case import *


class OracleDB:
    ALIAS = 'ORACLE'

    def __init__(self, table_name):
        self.parent_path = os.getcwd()
        self.keys_data = self.parent_path + "/save/keys/save_keys"
        self.host = db_info[0]
        self.port = db_info[1]
        self.user = db_info[2]
        self.password = db_info[3]
        self.database = db_info[4]
        self.table_name = table_name
        self.db = self.__connect()
        self.csv_file = self.parent_path + "/save/{0}.csv".format(self.table_name)
        self.col_files = self.parent_path + "/save/col_name/tab_col"

    def __del__(self):
        try:
            self.db.close()
        except cx_Oracle.Error as e:
            print(e)

    def __connect(self):
        db = cx_Oracle.connect('{0}/{1}@{2}:{3}/{4}'.format(
            self.user, self.password, self.host, self.port, self.database))
        db.ping()
        return db

    def __execute(self, sql):
        cursor = self.db.cursor()
        cursor.execute(sql)
        results = cursor.fetchall()
        return results

    def get_pk_col(self):
        sql = primary_key.format(self.table_name)
        cols = self.__execute(sql)
        col_name = [",".join(x) for x in cols]
        return col_name

    def alter(self, file, old_str, new_str):
        with open(file, "r", encoding="utf-8") as f1, open("%s.bak" % file, "w", encoding="utf-8") as f2:
            for line in f1:
                f2.write(re.sub(old_str, new_str, line))
        os.remove(file)
        os.rename("%s.bak" % file, file)

    def decide_tz_cols(self):
        newcols = []
        sql = col_data_type.format(self.table_name)
        datatype = self.__execute(sql)
        read_file = open(self.col_files, "r")
        cols_rule = read_file.read()
        rule_list = cols_rule.split(',')
        rule_dict = {}
        for e in rule_list:
            rule_dict[e] = ""   # make null dict
        for col in datatype:
            col_name = col[0]
            data_type = col[1]
            new_col = col_name   # make new variable`
            tz_obj = re.search(r"WITH TIME ZONE", data_type)    # match timestamp with time zone type
            if data_type[0:9] == "TIMESTAMP":   # 需要处理timestamp tz的数据类型
                if tz_obj:
                    new_col = "to_char({0},'yyyy-mm-dd hh24:mi:ss.ff9 tzh:tzm')".format(col_name)
                else:
                    new_col = "to_char({0},'yyyy-mm-dd hh24:mi:ss.ff9')".format(col_name)
            elif data_type == "DATE":
                new_col = "to_char({0},'yyyy-mm-dd hh24:mi:ss')".format(col_name)
            rule_dict[col_name] = new_col   # make dict value is col_name
        for e in rule_list:
            newcols.append(rule_dict[e])
        return newcols

    def query(self):
        cols_name = ",".join(self.decide_tz_cols())
        pk = ",".join(self.get_pk_col())
        db_tz_sql = "select dbtimezone from dual"
        db_tz = self.__execute(db_tz_sql)
        set_tz = "alter session set time_zone = '{0}'".format(db_tz[0][0])
        cursor = self.db.cursor()
        cursor.execute(set_tz)
        sql = "select {0} from {1} order by {2}".format(cols_name, self.table_name, pk)
        cursor.execute(sql)
        res = cursor.fetchall()
        to_csv = open(self.csv_file, "w", encoding="utf-8")
        writer = csv.writer(to_csv)
        writer.writerow(open(self.col_files, "r").read().split(","))  # write csv title from data table columns
        [writer.writerow(list(row)) for row in res]
        to_csv.close()


if __name__ == '__main__':
    f = OracleDB(tab_name)
    # d = f.get_pk_col()
    # f.decide_tz_cols()
    # print(d)
    h = f.query()
    print(h)
