# encoding: utf-8
# author TurboChang

# KAFKA
bootstrap_servers = "59.110.219.31:9092"
topic = "dp_agent_1_DP_TEST_T1"
begin_time = "2021-08-20 15:00:45"
end_time = "2021-08-20 15:06:55"
range_time = 0.25

# ORACL
host = "39.105.17.117"
port = 1521
username = "dp_test"
password = "123456"
database = "orcl"
db_info = [host, port, username, password, database]
# tab_name = "T1"
tab_name = "T_TIMESTAMP"
column_name = "TIME3"
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

