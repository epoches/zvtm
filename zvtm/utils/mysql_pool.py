#pymysql使用连接池，为了多个策略同时运行，共享连接池。


import pymysql
from dbutils.persistent_db import PersistentDB
import pandas as pd
import time
import socket
import logging

class MysqlPool(object):
    def __init__(self, host, user, passwd, db, port, charset):
        self.host = host
        self.port = port
        self.POOL = PersistentDB(
            creator=pymysql,  # 使用链接数据库的模块
            maxusage=None,  # 一个链接最多被重复使用的次数，None表示无限制
            setsession=[],  # 开始会话前执行的命令列表。如：["set datestyle to ...", "set time zone ..."]
            ping=7,
            cursorclass=pymysql.cursors.DictCursor,
            # ping MySQL服务端，检查是否服务可用。# 如：0 = None = never, 1 = default = whenever it is requested, 2 = when a cursor is created, 4 = when a query is executed, 7 = always
            host=host,
            port=port,
            user=user,
            password=passwd,
            database=db,
            connect_timeout=100,
            charset=charset
        )
        self.logger = self._init_logger()

    def connect(self):
        '''
        启动连接
        :return:
        '''
        retry_count = 10
        init_connect_count = 0
        while  init_connect_count < retry_count:
            try:
                conn = self.POOL.connection()
                cursor = conn.cursor()
                # 连接上退出循环，连接不上继续重连
                break
            except pymysql.Error as e:
                # self.logger.error(f"连接MySQL服务器出错：{e},重试{init_connect_count}次")
                # self._reCon()
                time.sleep(1)
                init_connect_count += 1
        if conn is None or cursor is None:
            raise Exception("无法连接到MySQL服务器")
        return conn, cursor

    def connect_close(self, conn, cursor):
        '''
        关闭连接
        :param conn:
        :param cursor:
        :return:
        '''
        try:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
        except Exception as e:
            self.logger.error(f"关闭连接出错：{e}")

    def fetch_all(self, return_type='df', sql=None, args=None):
        '''
        批量查询
        :param return_type: 返回结果类型，'df' 表示返回 DataFrame，其他值表示返回列表
        :param sql: SQL 查询语句
        :param args: 查询参数
        :return: 查询结果
        '''

        conn, cursor = self.connect()

        try:
            cursor.execute(sql, args)
            if return_type == 'df':
                result = pd.DataFrame.from_records(cursor.fetchall())
            else:
                result = cursor.fetchall()
            return result
        except pymysql.Error as e:
            self.logger.error(f"查询出错：{e}")
            raise
        finally:
            self.connect_close(conn, cursor)

    def _init_logger(self):
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)

        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(logging.INFO)
        stream_handler.setFormatter(formatter)

        logger.addHandler(stream_handler)

        return logger

    def fetch_one(self, sql, args):
        '''
        查询单条数据
        :param sql:
        :param args:
        :return:
        '''
        conn, cursor = self.connect()
        try:
            cursor.execute(sql, args)
            result = cursor.fetchone()
            return result
        except pymysql.Error as e:
            self.logger.error(f"查询出错：{e}")
            raise
        finally:
            self.connect_close(conn, cursor)

    def insert(self, sql, args):
        '''
        插入数据
        :param sql:
        :param args:
        :return:
        '''
        conn, cursor = self.connect()
        try:
            cursor.execute(sql, args)
            conn.commit()
            return cursor.rowcount
        except pymysql.Error as e:
            self.logger.error(f"插入数据出错：{e}")
            raise
        finally:
            self.connect_close(conn, cursor)

    def insertmany(self, sql, args):
        '''
        向数据表插入多条记录
        :param sql: 要插入的 SQL 格式
        :param args: 要插入的记录数据 tuple(tuple)/list[list]
        :return: 受影响的行数
        '''
        conn, cursor = self.connect()
        try:
            cursor.executemany(sql, args)
            conn.commit()
            return cursor.rowcount
        except pymysql.Error as e:
            self.logger.error(f"插入多条数据出错：{e}")
            raise
        finally:
            self.connect_close(conn, cursor)

    def delete_one(self, sql, args):
        conn, cursor = self.connect()
        try:
            cursor.execute(sql, args)
            conn.commit()
            return cursor.rowcount
        except pymysql.Error as e:
            self.logger.error(f"删除数据出错：{e}")
            raise
        finally:
            self.connect_close(conn, cursor)

    def update_one(self, sql, args):
        conn, cursor = self.connect()
        try:
            cursor.execute(sql, args)
            conn.commit()
            return cursor.rowcount
        except pymysql.Error as e:
            self.logger.error(f"更新数据出错：{e}")
            raise
        finally:
            self.connect_close(conn, cursor)



# import pymysql
# import pandas as pd
# from DBUtils.PooledDB import PooledDB, SharedDBConnection
# from pymysql.cursors import DictCursor
# import time
# import socket
# # from singleton_decorator import singleton
# '''
# 连接池
# '''
# # @singleton
# class MysqlPool(object):
#     def __init__(self, host, user, passwd, db, port, charset):
#         self.host = host
#         self.port = port
#         self.POOL = PooledDB(
#             creator=pymysql,  # 使用链接数据库的模块
#             maxconnections=60,  # 连接池允许的最大连接数，0和None表示不限制连接数
#             mincached=20,  # 初始化时，链接池中至少创建的空闲的链接，0表示不创建
#             maxcached=50,  # 链接池中最多闲置的链接，0和None不限制
#             maxshared=30,
#             # 链接池中最多共享的链接数量，0和None表示全部共享。PS: 无用，因为pymysql和MySQLdb等模块的 threadsafety都为1，所有值无论设置为多少，_maxcached永远为0，所以永远是所有链接都共享。
#             blocking=True,  # 连接池中如果没有可用连接后，是否阻塞等待。True，等待；False，不等待然后报错
#             maxusage=None,  # 一个链接最多被重复使用的次数，None表示无限制
#             setsession=[],  # 开始会话前执行的命令列表。如：["set datestyle to ...", "set time zone ..."]
#             ping=7,
#             cursorclass=DictCursor,
#             # ping MySQL服务端，检查是否服务可用。# 如：0 = None = never, 1 = default = whenever it is requested, 2 = when a cursor is created, 4 = when a query is executed, 7 = always
#             host=host,
#             port=port,
#             user=user,
#             password=passwd,
#             database=db,
#             connect_timeout=10,
#             charset=charset
#         )
#         self.connections = []  # 连接对象列表
#     # def __new__(cls, *args, **kw):
#     #     '''
#     #     启用单例模式
#     #     :param args:
#     #     :param kw:
#     #     :return:
#     #     '''
#     #     if not hasattr(cls, '_instance'):
#     #         cls._instance = object.__new__(cls)
#     #     return cls._instance
#
#     def connect(self):
#         '''
#         启动连接
#         :return:
#         '''
#         try:
#             # conn = self.POOL.connection()
#             # cursor = conn.cursor()
#             # return conn, cursor
#             # if not self.connections:
#             #     self._reCon()
#             # conn = self.connections.pop()  # 从连接对象列表中弹出连接对象
#             # cursor = conn.cursor()
#             # return conn, cursor
#             if not self.connections:
#                 if not self._is_server_available():
#                     print("MySQL server is not available. Attempting to reconnect...")
#                     self._reCon()
#                 else:
#                     conn = self.POOL.connection()
#                     cursor = conn.cursor()
#                     return conn, cursor
#
#             conn = self.connections.pop()  # 从连接对象列表中弹出连接对象
#             cursor = conn.cursor()
#             return conn, cursor
#         except Exception as e:
#             print(e)
#             self._reCon()
#
#     def _is_server_available(self):
#         """
#         检查服务器是否可用
#         :return: bool
#         """
#         try:
#             sock = socket.create_connection((self.host, self.port), timeout=10)
#             sock.close()
#             return True
#         except socket.error:
#             return False
#
#     def _reCon(self):
#         """ MySQLdb.OperationalError异常"""
#         # self.con.close()
#         # while True:
#         #     try:
#         #         time.sleep(5)  # 等待一段时间后进行重连
#         #         conn = self.POOL.connection()
#         #         cursor = conn.cursor()
#         #         print("重新连接成功")
#         #         return conn, cursor
#         while True:
#             try:
#                 time.sleep(5)  # 等待一段时间后进行重连
#                 conn = self.POOL.connection()
#                 cursor = conn.cursor()
#                 # print("重新连接成功")
#                 self.connections.append(conn)  # 将连接对象添加到列表中
#                 return conn, cursor
#             except Exception as e:
#                 print(f"重连失败：{e}")
#
#     def connect_close(self,conn, cursor):
#         '''
#         关闭连接
#         :param conn:
#         :param cursor:
#         :return:
#         '''
#         try:
#             if cursor:
#                 cursor.close()
#         finally:
#             if conn:
#                 # conn.close()
#                 self.connections.append(conn)  # 将连接对象重新添加到列表中
#     # 默认返回dataframe 标记不为df返回list
#     def fetch_all(self,
#                   return_type: str = 'df',
#                   sql: str = None,
#                   args: str = None):
#         '''
#         批量查询
#         :param sql:
#         :param args:
#         :return:
#         '''
#         # if not self._is_server_available():
#         #     self._reCon()
#         # conn, cursor = self.connect()
#         #
#         # cursor.execute(sql, args)
#         # record_list = cursor.fetchall()
#         # self.connect_close(conn, cursor)
#         # if return_type == 'df':
#         #     df = pd.DataFrame.from_records(record_list)
#         #     return df
#         # else:
#         #     return record_list
#         if not self._is_server_available():
#             self._reCon()
#             # raise ConnectionError("MySQL server is not available")
#
#         conn, cursor = self.connect()
#         try:
#             cursor.execute(sql, args)
#             if return_type == 'df':
#                 result = pd.DataFrame.from_records(cursor.fetchall())
#             else:
#                 result = cursor.fetchall()
#             return result
#         except pymysql.Error as e:
#             print('查询出错:%s' % (e))
#             raise
#         finally:
#             self.connect_close(conn, cursor)
#
#
#     def fetch_one(self,sql, args):
#         '''
#         查询单条数据
#         :param sql:
#         :param args:
#         :return:
#         '''
#         conn, cursor = self.connect()
#         cursor.execute(sql, args)
#         result = cursor.fetchone()
#         self.connect_close(conn, cursor)
#
#         return result
#
#     def insert(self,sql, args):
#         '''
#         插入数据
#         :param sql:
#         :param args:
#         :return:
#         '''
#         conn, cursor = self.connect()
#         row = cursor.execute(sql, args)
#         conn.commit()
#         self.connect_close(conn, cursor)
#         return row
#
#     def insertmany(self,sql, args):
#         '''
#         @summary: 向数据表插入多条记录
#         @param sql:要插入的ＳＱＬ格式
#         @param values:要插入的记录数据tuple(tuple)/list[list]
#         @return: count 受影响的行数
#         '''
#         try:
#             conn, cursor = self.connect()
#             row = cursor.executemany(sql, args)
#             conn.commit()
#             self.connect_close(conn, cursor)
#             return row
#         except Exception as e:
#             return False
#
#     def delete_one(self,sql,args):
#         conn, cursor = self.connect()
#         result = cursor.execute(sql,args)
#         conn.commit()
#         self.connect_close(conn, cursor)
#         return result
#
#     def update_one(self,sql,args):
#         try:
#             conn, cursor = self.connect()
#             result = cursor.execute(sql,args)
#             conn.commit()
#             self.connect_close(conn, cursor)
#             return result
#         except Exception as e:
#             return False
#
#     def updatemany(self,sql, args):
#         '''
#         @summary: 向数据表插入多条记录
#         @param sql:要插入的ＳＱＬ格式
#         @param values:要插入的记录数据tuple(tuple)/list[list]
#         @return: count 受影响的行数
#         '''
#         try:
#             conn, cursor = self.connect()
#             row = cursor.executemany(sql, args)
#             conn.commit()
#             cursor.close()
#             self.connect_close(conn, cursor)
#             return row
#         except Exception as e:
#             return False
#
#     def begin(self):
#         """开启事务"""
#         self._conn.autocommit(0)
#
#     def end(self, option='commit'):
#         """结束事务"""
#         if option == 'commit':
#             self._conn.autocommit()
#         else:
#             self._conn.rollback()
#
from zvtm import zvt_config
if __name__ == '__main__':
    # 实例化
    db = 'em_stock_1d_hfq_kdata'
    mp = MysqlPool(zvt_config['mysql_host'], zvt_config['mysql_user'], zvt_config['mysql_password'],db,int(zvt_config['mysql_port']),'utf8')

    '''
    查询单条
    '''
    # sql = "select * from stock_1d_hfq_kdata where code=%s limit 10"
    # #data = mp.fetch_one("select * from sys_user where user_name=%s", (username,))
    # data = mp.fetch_one(sql,'000001')
    # print(data)
#     for item in data:
#       print("0:%s,1:%s" % (item[0], item[1]))
#
#     '''
#     批量读取
#
#     '''
#     #sql = "select code,open,close,high,low,volume from stock_1d_hfq_kdata where code=%s limit 10"  #% ('000001')
    sql = "select code,open,close,high,low,volume from stock_1d_hfq_kdata where timestamp = %s"  # % ('000001')
    data = mp.fetch_all(sql=sql,args='2025-03-20')
    print(data)
#     #data = mp.fetch_all(return_type='',sql=sql,args='002932')
#     #print(data)
#     #for item in data:
#     #print(item) #data[0]['id']  item['id']
#
#     '''
#     插入数据
#     '''
#
#     #data = mp.insert("insert into record(line,ctime,user_id)values(%s,%s,%s)", (22, '2019-11-11', 1))
#     #print(data)
#
#     # 批量插入
#     """
#     批量增加数据
#     数据格式：[('0', 'xiaxuan'), ('1', 'xiaxuan'), ('2', 'xiaxuan')]
#     执行方式：db.insertmany("INSERT INTO demo (demo.age,demo.`name`) VALUES (%s, %s)",values)
#     :return: 执行条数
#     # """
#     # db = 'stock'
#     # mp = MysqlPool(schedule_config['mysql_host'], schedule_config['mysql_user'], schedule_config['mysql_password'], db,
#     #                int(schedule_config['mysql_port']), 'utf8')
#     #sql = "insert into orders(id,timestamp,trader_name)values(%s,%s,%s)"
#     #arg = [('11', '2019-11-11','pjz'),('22', '2021-11-11','pjz')]
#     # values = []
#     # for i in range(len(arg)):
#     #     value = (arg[i])
#     #     values.append(value)
#     #args = eval("('" + "','".join(values) + "')")
#     #data = mp.insertmany(sql=sql,args=arg)
#     #print(data)
#
#     # sql = "delete from stu where name = %s"   #删除
#     # res = delete_one(sql, "哪吒")
#     # print(res)
#
#     # sql = "UPDATE orders set  status=%s where id = %s"   #更新
#     # obj = ["b","600512_2022-04-08 00:00:00"]
#     # res = mp.update_one(sql,obj)
#     # print(res)
#
#     # 批量更新、
#     # db = 'em_valuation1'
#     # mp = MysqlPool(schedule_config['mysql_host'], schedule_config['mysql_user'], schedule_config['mysql_password'], db,
#     #                int(schedule_config['mysql_port']), 'utf8')
#     # sql = 'UPDATE stock_valuation set capitalization=%s,circulating_cap=%s where code = %s'
#     # arg = [
#     #     (29352200000, 29352200000, 600000),
#     #     (2816460000, 2816460000, 600001)
#     # ]
#     # data = mp.updatemany(sql=sql,args=arg)
#     # print(data)
#
# # the __all__ is generated
__all__ = ['MysqlPool']