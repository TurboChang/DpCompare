# encoding: utf-8
# author TurboChang

import os
import pymssql
import traceback
import datetime
from DBUtils.PooledDB import PooledDB
from concurrent.futures import ThreadPoolExecutor, as_completed


DB_LOGGING_START = '-' * 10 + 'DB-START' + '-' * 10
DB_LOGGING_END = '-' * 11 + 'DB-END' + '-' * 11

get_current_version = "SELECT CHANGE_TRACKING_CURRENT_VERSION() AS CURRENT_VERSION"
get_pk_cols = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE TABLE_NAME = 'test' ORDER BY ORDINAL_POSITION"
get_pk_dats = "select %s from CHANGETABLE( CHANGES test, {0}) as ct"
get_d_types = """select b.name,
                        case c.name 
                            when 'numeric' then 'numeric(' + convert(varchar,b.length) + '，' + convert(varchar,b.xscale) + ')'
                            when 'char' then 'char(' + convert(varchar,b.length) + ')'
                            when 'varchar' then 'varchar(' + convert(varchar,b.length) + ')'
                        else c.name END AS col_type
                from sysobjects a,syscolumns b,systypes c where a.id=b.id
                    and a.name='test' 
                    and a.xtype='U'
                    and b.xtype=c.xtype
                    and b.name in (%s)"""

def db_call(func):
    """
    数据库调用装饰器，用于打印行为的用时
    """
    def inner_wrapper(*args, **kwargs):
        print(DB_LOGGING_START)
        start_time = datetime.datetime.now()
        has_error = None
        ret = None
        try:
            ret = func(*args, **kwargs)
        except Exception as e:
            has_error = e
        stop_time = datetime.datetime.now()
        ms = (stop_time - start_time).seconds
        print('Query time: {0}s'.format(str(ms)))
        print(DB_LOGGING_END)
        if has_error:
            raise has_error
        return ret
    return inner_wrapper

# 基础类
class Base:

    def __init__(self, proc, batch):
        self.proc = proc
        self.batch = batch
        self.pool = self.create_pool()
        self.executor = ThreadPoolExecutor(max_workers=self.proc)
        self.file_name = os.getcwd() + "/ct_version"
        self.old_ct_version = self.__old_ct_version()

    def __del__(self):
        self.pool.close()

    def create_pool(self):
        """
        创建数据库连接池
        :return: 连接池
        """
        pool = PooledDB(creator=pymssql,
                        maxconnections=0,  # 连接池允许的最大连接数，0和None表示不限制连接数
                        mincached=4,  # 初始化时，链接池中至少创建的空闲的链接，0表示不创建
                        maxcached=0,  # 链接池中最多闲置的链接，0和None不限制
                        maxusage=None,  # 一个链接最多被重复使用的次数，None表示无限制
                        blocking=True,  # 连接池中如果没有可用连接后，是否阻塞等待。True，等待；False，不等待然后报错
                        host="60.205.227.46",  # 此处必须是是60.205.227.46  192.168.1.105
                        port=1433,
                        user="dp_test",
                        password="123456",
                        database="dp_test",
                        charset="utf8")
        return pool

    def __query_mssql(self, sql):
        try:
            db = self.pool.connection()  # 连接数据池
            cursor = db.cursor()  # 获取游标
            cursor.execute(sql)
            res = cursor.fetchall()
            cursor.close()
            db.close()
            return res
        except:
            traceback.print_exc()

    def __decide_data_type(self, param, list):
        # decide pk cols data type
        newcols = []
        rule_dict = {}
        for e in list:
            rule_dict[e[0]] = ""  # make null dict : {'col_date': '', 'id': ''}
        for col in list:
            col_name = col[0]
            data_type = col[1]
            new_col = param + col_name
            if data_type == "date":
                new_col = "convert(varchar, {0}{1})".format(param, col_name)
            rule_dict[
                col_name] = new_col  # new_col's value to rule_dict : {'col_date': 'convert(varchar, ct.col_date)', 'id': 'ct.id'}
        for e in list:
            newcols.append(rule_dict[e[0]])  # append values from rule_dict's key
        return newcols

    def __list_of_group(self, list_info, per_list_len):
        list_of_group = zip(*(iter(list_info),) * per_list_len)
        end_list = [list(i) for i in list_of_group]  # i is a tuple
        count = len(list_info) % per_list_len
        end_list.append(list_info[-count:]) if count != 0 else end_list
        return end_list

    def __file_io(self, param):
        file = open(self.file_name, param)
        return file

    def __old_ct_version(self):
        # get old ct version
        size = os.path.getsize(self.file_name)
        print("size: {0}".format(str(size)) )
        if size == 0:
            version = self.__query_mssql(get_current_version)[0][0]
            return version
        else:
            file = self.__file_io("r")
            version = file.read()
            return version

    def query_datas(self):
        pk_cols = self.__query_mssql(get_pk_cols)

        # get pk cols data type
        cols_list = [col[0] for col in pk_cols if pk_cols]
        d_sql_cols = (','.join("'" + item + "'" for item in cols_list))
        d_type_sql = get_d_types % d_sql_cols
        d_type= self.__query_mssql(d_type_sql)

        # get current ct version
        current_ct_version = self.__query_mssql(get_current_version)[0][0]
        print(current_ct_version, self.old_ct_version)
        if int(current_ct_version) > int(self.old_ct_version):

            # get pk values from ct
            newcols = self.__decide_data_type("ct.", d_type)
            ct_cols = ", ".join([col for col in newcols if newcols])
            ct_sql = get_pk_dats.format(self.old_ct_version) % ct_cols
            print(ct_sql)
            ct_datas = self.__query_mssql(ct_sql) # ct_datas : ('2021-09-08', 356312)

            # real time query
            condition_list = self.__decide_data_type("", d_type)
            w_condition_dict = {}
            w_values_list = []
            for row in ct_datas:
                for x, y in zip(condition_list, list(row)):
                    w_condition_dict[x] = y
                    res = " and ".join([k + "='" + str(w_condition_dict[k]) + "'" for k in w_condition_dict if w_condition_dict])
                    w_values_list.append(res)
            end_list = self.__list_of_group(w_values_list, 50000)
            for li in end_list:
                where_sql = " or ".join(li)
                reel_time_sql = "select * from test where {0}".format(where_sql)
                results = self.__query_mssql(reel_time_sql)
                # print(results)
                # self.__query_mssql(reel_time_sql)

    def save_mssql(self, sql, args):
        """
        保存数据库
        :param sql: 执行sql语句
        :param args: 添加的sql语句的参数 list[tuple]
        """
        try:
            db = self.pool.connection()  # 连接数据池
            cursor = db.cursor()  # 获取游标
            cursor.executemany(sql, args)
            db.commit()
            cursor.close()
            db.close()
        except:
            traceback.print_exc()

    # 插入数据
    def insertdata(self, data):
        values_list = []
        sql = "insert into test (col1, col2, col3, col4, col5, col6, col7, col8, col9) values (%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        args = tuple([data for i in range(9) if id and data])
        [values_list.append(args) for i in range(self.batch) if args]
        self.save_mssql(sql, values_list)
        current_ct_version = str(self.__query_mssql(get_current_version)[0][0])
        fileio = self.__file_io("w")
        fileio.write(current_ct_version)

    # @db_call
    def find_all_done(self):
        tasks = []
        datas = []

        # submit函数来提交线程需要执行的任务（函数名和参数）到线程池中，不阻塞
        [tasks.append(self.executor.submit(self.insertdata, "xxx")) for i in range(self.proc) if self.proc]
        [tasks.append(self.executor.submit(self.query_datas,)) for i in range(self.proc) if self.proc]
        # as_completed()是ThreadPoolExecutor中的方法，用于取出所有任务的结果
        [datas.append(future.result()) for future in as_completed(tasks)]


if __name__ == '__main__':
    f = Base(2, 100)
    f.find_all_done()
    # f.insertdata("xxx")
    # g = f.query_datas()
    # print(g)




