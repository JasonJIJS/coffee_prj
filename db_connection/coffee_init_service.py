import os
from configparser import ConfigParser
from mysql.connector import Error, errorcode
from mysql.connector.pooling import MySQLConnectionPool

from db_connection.db_connection import ConnectionPool

# DB_NAME = 'coffeetest'
# TABLES = {'product':(
#     """
#     create table product(
#         code char(4) not null,
#         name varchar(20) not null,
#         primary key(code))
#     """
#
# ), 'sale':(
#     """
#     create table sale(
#         no int(11) auto_increment,
#         code char(4) not null,
#         price int(11) not null,
#         saleCnt int(11) not null,
#         marginRate int(11) not null,
#         primary key(no),
#         foreign key(code) references product(code))
#     """
# )}


class DBInitservice:
    OPTION = """
        CHARACTER SET 'UTF8'
        FIELDS TERMINATED by ','
        LINES TERMINATED by '\r\n'
        """

    def __init__(self, source_dir='data/', data_dir='data/'):
        self._db = read_ddl_file()
        self.source_dir = source_dir
        self.data_dir = data_dir



    def read_ddl_file(self, filename = 'database_setting/coffee_ddl.ini'):
        parser = ConfigParser()
        parser.read(filename, encoding='UTF8')

        db = {}
        for sec in parser.sections():
            items = parser.items(sec)
            if sec == 'name':
                for key, value in items:
                    db[key] = value

            if sec == 'sql':
                sql = {}
                for key, value in items:
                    sql[key] = "".join(value.splitlines())
                db['sql'] = sql

            if sec == 'trigger':
                trigger = {}
                for key, value in items:
                    trigger[key] = " ".join(value.splitlines())
                db['trigger'] = trigger

            if sec == 'procedure':
                procedure = {}
                for key, value in items:
                    procedure[key] = " ".join(value.splitlines())
                db['procedure'] = procedure

            if sec == 'sql_select':
                sql_select = {}
                for key, value in items:
                    sql_select[key] = " ".join(value.splitlines())
                db['sql_select'] = sql_select

            if sec == 'user':
                for key, value in items:
                    db[key] = value

        return db


    def __create_database(self):
        try:
            sql = read_ddl_file()
            conn = ConnectionPool.get_instance().get_connection()
            cursor = conn.cursor()
            cursor.execute("CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(self._db['database_name']))
            print("CREATE DATABASE {}".format(self._db['database_name']))
        except Error as err:
            if err.errno == errorcode.ER_DB_CREATE_EXISTS:
                cursor.execute("DROP DATABASE {} ".format(self._db['database_name']))
                print("DROP DATABASE {}".format(self._db['database_name']))
                cursor.execute("CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(self._db['database_name']))
                print("CREATE DATABASE {}".format(self._db['database_name']))
            else:
                print(err.msg)
        finally:
            cursor.close()
            conn.close()


    def __create_table(self):
        try:
            conn = ConnectionPool.get_instance().get_connection()
            cursor = conn.cursor()
            cursor.execute("USE {}".format(self._db['database_name']))
            for table_name, table_sql in self._db['sql'].items():
                try:
                    print("Creating table {}:".format(table_name), end='')
                    cursor.execute(table_sql)
                except Error as err:
                    if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                        print("already exists.")
                    else:
                        print(err.msg)
                else:
                    print("OK")
        except Error as err:
            print(err)
        finally:
            cursor.close()
            conn.close()

    def __create_user(self):
        try:
            conn = ConnectionPool.get_instance().get_connection()
            cursor = conn.cursor()
            print("Creating user: ", end='')
            cursor.execute(self._db['user_sql'])
            print("OK")
        except Error as err:
            print(err)
        finally:
            cursor.close()
            conn.close()


    def __create_trigger(self):
        try:
            conn = ConnectionPool.get_instance().get_connection()
            cursor = conn.cursor()
            cursor.execute("USE {}".format(self._db['database_name']))
            for trigger_name, trigger_sql in self._db['trigger'].items():
                try:
                    print("Creating trigger {}:".format(trigger_name), end='')
                    cursor.execute(trigger_sql)
                except Error as err:
                    if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                        print("already exists.")
                    else:
                        print(err.msg)
                else:
                    print("OK")

        except Error as err:
            print(err)
        finally:
            cursor.close()
            conn.close()


    def __create_procedure(self):
        try:
            conn = ConnectionPool.get_instance().get_connection()
            cursor = conn.cursor()
            cursor.execute("USE {}".format(self._db['database_name']))
            for procedure_name, procedure_sql in self._db['procedure'].items():
                try:
                    print("Creating procedure {}:".format(procedure_name), end='')
                    cursor.execute(procedure_sql)
                except Error as err:
                    if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                        print("already exists.")
                    else:
                        print(err.msg)
                else:
                    print("OK")

        except Error as err:
            print(err)
        finally:
            cursor.close()
            conn.close()


    def __sql_select(self):
        try:
            conn = ConnectionPool.get_instance().get_connection()
            cursor = conn.cursor()
            cursor.execute("USE {}".format(self._db['database_name']))
            for sql_select_name, sql_select_sql in self._db['sql_select'].items():
                try:
                    print("Creating sql_select {}:".format(sql_select_name), end='')
                    cursor.execute(sql_select_sql)
                except Error as err:
                    if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                        print("already exists.")
                    else:
                        print(err.msg)
                else:
                    print("OK")
                    print(sql_select_sql)
        except Error as err:
            print(err)
        finally:
            cursor.close()
            conn.close()





    def data_backup(self, table_name):
        filename = table_name + '.txt'

        try:
            conn = ConnectionPool.get_instance().get_connection()
            cursor = conn.cursor()
            cursor.execute("USE {}".format(self._db['database_name']))
            source_path = os.path.abspath(self.data_dir + filename).replace('\\', '/')

            # source_path = self.source_dir + filename
                # print('source_path =', source_path)

                # if os.path.exists(source_path):
                #     os.remove(source_path)

            backup_sql = "SELECT * FROM {} INTO OUTFILE '{}' {}".format(table_name, source_path,
                                                                            DBInitservice.OPTION)
            # print("backup_sql ", backup_sql)
            cursor.execute(backup_sql)

            print(table_name, "backup complete!")
        except Error as err:
            print(err)
        finally:
            if conn.is_connected():
                cursor.close()
                conn.close()

    def data_restore(self, table_name):
        filename = table_name + '.txt'
        try:
            conn = ConnectionPool.get_instance().get_connection()
            cursor = conn.cursor()
            cursor.execute("USE {}".format(self._db['database_name']))
            data_path = os.path.abspath(self.data_dir + filename).replace('\\', '/')

            if not os.path.exists(data_path):
                    print("파일 '{}' 이 존재하지 않음".format(data_path))
                    return
            restore_sql = "LOAD DATA INFILE '{}' INTO TABLE {} {}".format(data_path, table_name,
                                                                              DBInitservice.OPTION)  # ubuntu
            cursor.execute(restore_sql)
            conn.commit()
            print(table_name, "restore complete!")
        except Error as err:
            print(err)
            print(table_name, "restore Fail!")
        finally:
            if conn.is_connected():
                cursor.close()
                conn.close()


    def service(self):
        self.__create_database()
        self.__create_table()
        self.__create_procedure()
        self.__create_trigger()
        self.__create_user()
        # self.data_backup('product')
        # self.data_backup('sale')
        # self.data_restore('product')
        # self.data_restore('sale')






def read_ddl_file(filename = 'database_setting/coffee_ddl.ini'):
    parser = ConfigParser()
    parser.read(filename, encoding='UTF8')

    db = {}
    for sec in parser.sections():
        items = parser.items(sec)
        if sec == 'name':
            for key, value in items:
                db[key] = value

        if sec == 'sql':
            sql = {}
            for key, value in items:
                sql[key] = "".join(value.splitlines())
            db['sql'] = sql

        if sec == 'trigger':
            trigger = {}
            for key, value in items:
                trigger[key] = " ".join(value.splitlines())
            db['trigger'] = trigger

        if sec == 'procedure':
            procedure = {}
            for key, value in items:
                procedure[key] = " ".join(value.splitlines())
            db['procedure'] = procedure

        if sec == 'sql_select':
            sql_select = {}
            for key, value in items:
                sql_select[key] = " ".join(value.splitlines())
            db['sql_select'] = sql_select

        if sec == 'user':
            for key, value in items:
                db[key] = value

    return db





"""
Ubuntu Linux 

SHOW VARIABLES LIKE "secure_file_priv";

/*
확인후 추가
[mysqld]
secure_file_priv=""
*/

원하는 경로에 (Errcode: 13 - Permission denied) 해결책 https://dreamlog.tistory.com/563
/etc/apparmor.d/usr.sbin.mysqld를 편집

추가
# Allow data files dir access
  /var/lib/mysql-files/ r,
  /var/lib/mysql-files/** rwk,

  /home/work/PycharmProjects/python_mysql_study/restore_bakup/data/ r,
  /home/work/PycharmProjects/python_mysql_study/restore_bakup/data/** rwk,
  /home/ji/PycharmProjects/coffee_prj/data/ r,
  /home/ji/PycharmProjects/coffee_prj/data/** rw,


/etc/init.d/apparmor restart

MySQL 재실행 후 외부 파일 읽기가 정상동작하는 것을 확인

"""