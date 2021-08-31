# encoding: utf-8
# author TurboChang

import os
import sys
from dp_to_csv import StoreKafka as SK
from dp_oracle import OracleDB as Ora
base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(base_dir)
from assets.conf_case import *

f = Ora(tab_name)
pk = f.get_pk_col()
# print(pk)
# d_type = f.get_tz()
# print(d_type)
# o_type = f.get_data_type()
# print(o_type)

g = SK(pk)
g.store_data()