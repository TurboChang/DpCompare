# encoding: utf-8
# author TurboChang

import os
import re
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
        self.keys_data = self.parent_path + "/save/keys/save_keys"
        self.host = db_info[0]
        self.port = db_info[1]
        self.user = db_info[2]
        self.password = db_info[3]
        self.database = db_info[4]
        self.table_name = table_name
        self.db = self.__connect()
        self.csv_file = self.parent_path + "/save/{0}.csv".format(self.table_name)
        self.col_files = self.parent_path + "/save/col_name/{0}_COLS".format(self.table_name)

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
        oldcols = []
        newcols = []
        sql = col_data_type.format(self.table_name)
        datatype = self.__execute(sql)
        for col in datatype:
            col_name = col[0]
            data_type = col[1]
            if data_type[0:9] == "TIMESTAMP":
                new_col = "to_char({0},'yy-mm-dd hh24:mi:ss.ff')".format(col_name)
                oldcols.append(col_name)
                newcols.append(new_col)
            elif data_type == "DATE":
                new_col = "to_char({0},'yy-mm-dd hh24:mi:ss')".format(col_name)
                oldcols.append(col_name)
                newcols.append(new_col)
        print(oldcols)
        print(newcols)

    def query(self):
        read_file = open(self.col_files, "r")
        cols_name = read_file.read()
        print(cols_name)
        db_tz_sql = "select dbtimezone from dual"
        db_tz = self.__execute(db_tz_sql)
        set_tz = "alter session set time_zone = '{0}'".format(db_tz[0][0])
        cursor = self.db.cursor()
        cursor.execute(set_tz)
        sql = "select {0} from {1} order by ID".format(cols_name, self.table_name)
        print(sql)
        cursor.execute(sql)
        res = cursor.fetchall()
        return res

if __name__ == '__main__':
    f = OracleDB(tab_name)
    d = f.get_pk_col()
    f.decide_tz_cols()
    print(d)
    # h = f.query()
    # print(h)