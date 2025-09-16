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
        # 初始化变量，避免 UnboundLocalError
        conn = None
        cursor = None
        retry_count = 10
        init_connect_count = 0
        while  init_connect_count < retry_count:
            try:
                conn = self.POOL.connection()
                cursor = conn.cursor()
                # 连接上退出循环，连接不上继续重连
                break
            except pymysql.Error as e:
                # 添加详细的错误信息
                error_msg = f"MySQL连接错误 ({e.args[0]}): {e.args[1]}"
                print(error_msg)  # 或使用日志记录
                raise Exception(f"无法连接到MySQL服务器: {error_msg}")
                time.sleep(1)
                init_connect_count += 1
            except Exception as e:
                error_msg = f"未知连接错误: {str(e)}"
                print(error_msg)
                raise Exception(f"无法连接到MySQL服务器: {error_msg}")
            # except pymysql.Error as e:
            #     # self.logger.error(f"连接MySQL服务器出错：{e},重试{init_connect_count}次")
            #     # self._reCon()
            #     time.sleep(1)
            #     init_connect_count += 1
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


from zvtm import init_log, zvt_config
def get_data(db,sql,arg):
    mp = MysqlPool(zvt_config['mysql_host'], zvt_config['mysql_user'], zvt_config['mysql_password'], db,
                   int(zvt_config['mysql_port']), 'utf8')
    df = mp.fetch_all(sql=sql, args=arg)
    return df


if __name__ == '__main__':
    # 实例化
    db = 'xt_stock_1d_hfq_kdata'
    mp = MysqlPool(zvt_config['mysql_host'], zvt_config['mysql_user'], zvt_config['mysql_password'],db,int(zvt_config['mysql_port']),'utf8')

    '''
    查询单条
    '''
    sql = "select * from stock_1d_hfq_kdata where code=%s limit 10"
    #data = mp.fetch_one("select * from sys_user where user_name=%s", (username,))
    data = mp.fetch_one(sql,'601788')
    print(data)

    db = 'em_stock_1d_hfq_kdata'
    sql = "select timestamp,code,open,close,high,low,volume from stock_1d_hfq_kdata where timestamp>= %s and timestamp <=%s and code = %s"
    arg = ['2025-01-01', '2025-03-01', '601398']
    df = get_data(db=db, sql=sql, arg=arg)
    print(df)
#     for item in data:
#       print("0:%s,1:%s" % (item[0], item[1]))
#
#     '''
#     批量读取
#
#     '''
#     #sql = "select code,open,close,high,low,volume from stock_1d_hfq_kdata where code=%s limit 10"  #% ('000001')
    sql = "select code,open,close,high,low,volume from stock_1d_hfq_kdata where timestamp > %s"  # % ('000001')
    data = mp.fetch_all(sql=sql,args='2025-08-01')
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