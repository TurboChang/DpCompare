# encoding: utf-8
# author TurboChang

import datetime
import os
import sys
import smtplib
from email.mime.text import MIMEText
base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(base_dir)
from assets.conf_case import *

report_file = r"core/report/compare.txt"

class DpMail:

    def __init__(self):
        self.current_time = datetime.datetime.now().strftime("%Y-%m-%d:%H%M%S")
        self.parent_path = os.getcwd()
        self.report_file_1 = r"/core/report/compare.txt"
        self.report_file = self.parent_path + self.report_file_1

    def __del__(self):
        old_name = self.parent_path + "/core/report/compare.txt"
        new_name = self.parent_path + "/core/report/compare-" + str(self.current_time) + ".txt"
        os.rename(old_name, new_name)

    def get_content(self):
        file_stat = os.stat(self.report_file).st_size
        if file_stat > 0:
            fo = open(self.report_file, "r")
            read = fo.readlines()
            str_convert = "</br>".join(read)
            return str_convert

    def sendmail(self):
        if not self.get_content() is None:
            contents = mail_content.format(self.get_content())
            msg = MIMEText(contents, "html", "utf-8")
            msg['Subject'] = subject
            msg['From'] = from_mail
            msg['To'] = ','.join(to_mail)
            msg["Cc"] = ','.join(cc_mail)
            server = smtplib.SMTP_SSL(host)
            server.connect(host, "465")
            server.login("clx@datapipeline.com", "P6HhQ7kcPfVZpxmV")
            server.sendmail(from_mail, to_mail + cc_mail, msg.as_string())
            server.quit()
            print(str(self.current_time) + "-邮件发送成功.")
        else:
            raise EmailException("邮件: {0} 发送失败".format(self.get_content()))
