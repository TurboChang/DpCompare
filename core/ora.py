# encoding: utf-8
# author TurboChang

import os
import sys
import cx_Oracle
import csv
from tzlocal import get_localzone
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

    def decide_tz_cols(self):
        cols = []
        sql = col_data_type.format(self.table_name)
        datatype = self.__execute(sql)
        for col in datatype:
            col_name = col[0]
            data_type = col[1]
            if data_type[0:9] == "TIMESTAMP":
                cols.append(col_name)
        return cols

    def decide_date_cols(self):
        cols = []
        sql = col_data_type.format(self.table_name)
        datatype = self.__execute(sql)
        for col in datatype:
            col_name = col[0]
            data_type = col[1]
            if data_type == "DATE":
                cols.append(col_name)
        return cols

    def query(self):
        db_tz_sql = "select dbtimezone from dual"
        db_tz = self.__execute(db_tz_sql)
        set_tz = "alter session set time_zone = '{0}'".format(db_tz[0][0])
        print(set_tz)
        cursor = self.db.cursor()
        cursor.execute(set_tz)
        sql = "select * from {0}".format(self.table_name)
        cursor.execute(sql)
        res = cursor.fetchall()
        return res

if __name__ == '__main__':
    f = OracleDB("T_TIMESTAMP")
    # d = f.get_pk_col()
    g = f.decide_tz_cols()
    # print(d)
    print(g)
    h = f.query()
    print(h)