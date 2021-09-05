# encoding: utf-8
# author TurboChang

# KAFKA
bootstrap_servers = "59.110.219.31:9092"
topic = "dp_agent_1_DP_TEST_T1_NEW"
begin_time = "2021-08-31 18:00:00"
end_time = "2021-08-31 20:00:00"
range_time = 0.25

# ORACL
host = "39.105.17.117"
port = 1521
username = "dp_test"
password = "123456"
database = "orcl"
db_info = [host, port, username, password, database]
# tab_name = "T1"
tab_name = "T1_NEW"
column_name = "col_ltz"
c_type = """
        SELECT COLUMN_NAME, 
               DATA_TYPE
FROM ALL_TAB_COLUMNS
WHERE TABLE_NAME = UPPER('{0}')
  AND OWNER = UPPER('{1}')
  AND COLUMN_NAME = '{2}'
ORDER BY COLUMN_ID
"""
primary_key = """
SELECT 
     COLUMN_NAME
FROM ALL_CONSTRAINTS TC
INNER JOIN ALL_CONS_COLUMNS KU
        ON TC.CONSTRAINT_TYPE = 'P'
       AND TC.CONSTRAINT_NAME = KU.CONSTRAINT_NAME
       AND KU.TABLE_NAME = UPPER('{0}')
       AND KU.OWNER = UPPER('"""+ username +"""')"""
col_data_type = """
SELECT COLUMN_NAME,
       DATA_TYPE 
 FROM ALL_TAB_COLUMNS KU 
WHERE KU.TABLE_NAME = UPPER('{0}')
  AND KU.OWNER = UPPER('""" + username + """')"""

# Mail
host = "smtp.exmail.qq.com"
subject = u"DataPipeline Agent-数据对比差异报告"
to_mail = ["clx@datapipeline.com"]
cc_mail = ["hanlin@datapipeline.com"]
from_mail = "clx@datapipeline.com"
mail_content ="""<table width="1500" border="0" cellspacing="0" cellpadding="4">
        <tr>
            <td bgcolor="CECFAD" headers="20" style="font-size: 14px">
                <br>*差异数据</br>
            </td>
        </tr>
        <tr>
            <td bgcolor="#EFEBDE" height="300" style="font-size: 13px">
                <br>差异明细:</br>
                {0}
            </td>
        </tr>
    </table>
        """

