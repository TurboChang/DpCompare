# encoding: utf-8
# author TurboChang

import os
import sys
import json
from csv_diff import load_csv, compare
base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(base_dir)
from assets.conf_case import *
from core.dp_ora import OracleDB as Ora

class CompareCSV:
    alias = "COMPARE"

    def __init__(self):
        self.current_path = os.path.dirname(__file__)
        self.csv_path = os.path.dirname(self.current_path) + "/core/save/"
        ora = Ora(tab_name)
        self.pk_list = ora.get_pk_col()
        self.pks = ",".join(self.pk_list)
        self.ora_csv = self.csv_path + tab_name + ".csv"
        self.kaf_csv = self.csv_path + topic + ".csv"
        self.report_file = os.path.dirname(self.current_path) + "/report/diff_report.txt"
        self.diff_datas = self.compare()

    def write_report(self):
        title = tab_name + " difference data is: "
        if not self.diff_datas is None:
            f = open(self.report_file, "a")
            [f.write(title + str(json.dumps(data, indent=2) + "\n\n")) for data in self.diff_datas]
            f.close()

    def compare(self):
        diff_datas_list = []
        ora_csv = open(self.ora_csv, "r")
        kaf_csv = open(self.kaf_csv, "r")
        source = load_csv(ora_csv, key=None)
        target = load_csv(kaf_csv, key=None)
        if target != {}:
            diff = compare(source, target)
            diff_str = "{'added': [], 'removed': [], 'changed': [], 'columns_added': [], 'columns_removed': []}"
            if str(diff) != diff_str:
                diff_datas_list.append(diff)
        ora_csv.close()
        kaf_csv.close()
        return diff_datas_list


if __name__ == '__main__':
    f = CompareCSV()
    # g = f.compare()
    # g = f.write_report()
    f.write_report()
    # print(g)
