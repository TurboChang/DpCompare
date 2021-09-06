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
from exception.related_exception import EmailException

class DpMail:

    def __init__(self):
        self.current_path = os.path.dirname(__file__)
        self.current_time = datetime.datetime.now().strftime("%Y-%m-%d:%H%M%S")
        self.parent_path = os.getcwd()
        self.report_file = os.path.dirname(self.current_path) + "/report/diff_report.txt"

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

if __name__ == '__main__':
    f = DpMail()
    f.sendmail()