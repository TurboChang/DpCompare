# encoding: utf-8
# author TurboChang

import os
import sys
from datetime import datetime, timedelta
base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(base_dir)
from assets.conf_case import *

def pk_2_utc(pktime_str: str, tz, param):
    global result
    now_time = datetime.strptime(pktime_str, "%Y-%m-%d %H:%M:%S")
    begin_utc = now_time - timedelta(hours=int(tz)) - timedelta(hours=range_time)
    end_utc = now_time - timedelta(hours=int(tz)) + timedelta(hours=range_time)
    if param == "begin":
        result = str(datetime.strptime(str(begin_utc), "%Y-%m-%d %H:%M:%S"))
    elif param == "end":
        result = str(datetime.strptime(str(end_utc), "%Y-%m-%d %H:%M:%S"))
    return result

if __name__ == '__main__':
    print(begin_time, end_time)
    f = pk_2_utc(begin_time, 8, "begin")
    g = pk_2_utc(end_time, 8, "end")
    print(f, g)