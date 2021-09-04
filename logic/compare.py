# encoding: utf-8
# author TurboChang

import os
import sys
from csv_diff import load_csv, compare
base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(base_dir)
from assets.conf_case import *
from core.dp_ora import OracleDB as Ora

class CompareCSV:

    def __init__(self):
        self.current_path = os.path.dirname(__file__)
        self.csv_path = os.path.dirname(self.current_path) + "/core/save/"
        ora = Ora(tab_name)
        self.pk_list = ora.get_pk_col()
        self.pks = ",".join(self.pk_list)

    def compare(self):
        ora_csv = open(self.csv_path + tab_name + ".csv", "r")
        kaf_csv = open(self.csv_path + topic + ".csv", "r")
        source = load_csv(ora_csv, key=self.pks)
        target = load_csv(kaf_csv, key=self.pks)
        if target != {}:
            print("TARGET IS NOT NULL.")
            diff = compare(source, target)
            diff_str = "{'added': [], 'removed': [], 'changed': [], 'columns_added': [], 'columns_removed': []}"
            if str(diff) != diff_str:
                print(diff)
                # self._write_report(report_file, content, "a")
        ora_csv.close()
        kaf_csv.close()


if __name__ == '__main__':
    f = CompareCSV()
    g = f.compare()
    print(g)
