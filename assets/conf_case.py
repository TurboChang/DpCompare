# encoding: utf-8
# author TurboChang

# KAFKA
bootstrap_servers = "59.110.219.31:9092"
topic = "dp_agent_1_DP_TEST_T1"
begin_time = "2021-08-20 15:00:45"
end_time = "2021-08-20 15:06:55"

# ORACLE
host = "39.105.17.117"
port = 1521
username = "dp_test"
password = "123456"
database = "orcl"
db_info = [host, port, username, password, database]
tab_name = "T1"
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
SELECT DATA_TYPE FROM ALL_TAB_COLUMNS KU 
WHERE KU.TABLE_NAME = UPPER('{0}')
  AND KU.COLUMN_NAME IN (%s)
  AND KU.OWNER = UPPER('""" + username + """')"""

