# encoding: utf-8
# author TurboChang

import os
import datetime

loops = 1000

begin = datetime.datetime.now()
current_path = os.getcwd() + "/"
exec_file = os.system(current_path + "multi_processes.py")
[exec_file for i in range(loops)]
end = datetime.datetime.now()

print("begin: " + str(begin))
print("end: " + str(end))
ms = (end - begin).seconds
print('Query time: {0}s'.format(str(ms)))