# encoding: utf-8
# author TurboChang

import pymssql
import traceback
import datetime
from DBUtils.PooledDB import PooledDB
from concurrent.futures import ThreadPoolExecutor, as_completed


DB_LOGGING_START = '-' * 10 + 'DB-START' + '-' * 10
DB_LOGGING_END = '-' * 11 + 'DB-END' + '-' * 11

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
    """
    用于连接和关闭
    """

    def __init__(self, proc, batch):
        self.batch = batch
        self.pool = self.create_pool()
        self.executor = ThreadPoolExecutor(max_workers=proc)

    def create_pool(self):
        """
        创建数据库连接池
        :return: 连接池
        """
        pool = PooledDB(creator=pymssql,
                        maxconnections=0,  # 连接池允许的最大连接数，0和None表示不限制连接数
                        mincached=4,  # 初始化时，链接池中至少创建的空闲的链接，0表示不创建
                        maxcached=0,  # 链接池中最多闲置的链接，0和None不限制
                        maxusage=1,  # 一个链接最多被重复使用的次数，None表示无限制
                        blocking=True,  # 连接池中如果没有可用连接后，是否阻塞等待。True，等待；False，不等待然后报错
                        host="60.205.227.46",  # 此处必须是是127.0.0.1
                        port=1433,
                        user="dp_test",
                        password="123456",
                        database="dp_test",
                        charset="utf8")
        return pool

    def save_mysql(self, sql, args):
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
    # @db_call
    def insertdata(self, data):
        values_list = []
        sql = "insert into test (col1, col2, col3, col4, col5, col6, col7, col8, col9) values (%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        args = tuple([data for i in range(9) if id and data])
        [values_list.append(args) for i in range(self.batch) if args]
        self.save_mysql(sql, values_list)

    @db_call
    def find_all_done(self):
        tasks = []
        datas = []
        tasks.append(self.executor.submit(self.insertdata, "xxx"))  # submit函数来提交线程需要执行的任务（函数名和参数）到线程池中，不阻塞
        [datas.append(future.result()) for future in as_completed(tasks)]  # as_completed()是ThreadPoolExecutor中的方法，用于取出所有任务的结果


if __name__ == '__main__':
    f = Base(4, 10000)
    f.find_all_done()
    # f.insertdata("xxx")




