#!/bin/python3
# -*- encoding: utf-8 -*-
"""
####################################################################################################
#  Name        :  archive_partition_table.py
#  Author      :  Elison
#  Email       :  Ly99@qq.com
#  Description :  归档分区表
#  Updates     :
#      Version     When            What
#      --------    -----------     -----------------------------------------------------------------
#      v1.0        2022-11-13
#      v1.1        2022-12-08      增加slow-copy、slow-replace等模式
####################################################################################################
"""
import sys
import time
import argparse
import logging
import util
from archiver import expr_to_date


def set_log_level(level='info'):
    "设置日志等级"
    if level == 'debug':
        lv = logging.DEBUG
    else:
        lv = logging.INFO
    logging.basicConfig(stream=sys.stdout, level=lv,
                        format='[%(asctime)s.%(msecs)d] [%(levelname)s] %(funcName)s: %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')


def get_args():
    '获取参数'
    parser = argparse.ArgumentParser()
    parser.add_argument("-v", "--version", action='store_true', help="查看版本")
    parser.add_argument("-S", "--source", type=str, required=True, help="源表所在的实例IP和端口, 如: 10.0.0.201:3306")
    parser.add_argument("-T", "--target", type=str, required=True, help="归档表所在的实例IP和端口, 如: 10.0.0.201:3306")
    parser.add_argument("-d", "--database", type=str, required=True,
                        help="归档表的数据库名，如：orderdb或orderdb:orderdb_history(可以使用冒号分别指定源端和目标端db名)")
    parser.add_argument("-t", "--table", type=str, required=True,
                        help="归档表的表名，如：orders或orders:orders_history(可以使用冒号分别指定源端和目标端table名)")
    parser.add_argument("-w", "--where", type=str, required=True, help="归档条件")
    parser.add_argument("-c", "--charset", default='utf8', choices=['utf8', 'utf8mb4', 'gbk'], help="归档条件")
    parser.add_argument("-u", "--user", type=str, default='dba_archive_user', help="用户名")
    parser.add_argument("-p", "--password", default='abc123', type=str, help="密码")
    parser.add_argument("-m", "--mode", default='copy', choices=['copy', 'slow-copy', 'slow-replace', 'no-copy'],
                        help="模式，copy:使用pt-archiver拷贝数据，slow-copy:兼容模式（不会丢数据），no-copy:不拷贝数据")
    parser.add_argument("-r", "--repeat", action='store_true', help="重复执行，直到异常退出")
    args = parser.parse_args()

    # 处理参数
    if args.version:
        print(__doc__)
        sys.exit()

    dct = {}
    try:
        source_host, source_port = args.source.split(':')
        source_host.split('.')[3]
        source_port = int(source_port)
        dct['source_host'] = source_host
        dct['source_port'] = source_port
    except Exception as e:
        print("无效参数：-S")
        sys.exit(1)

    try:
        target_host, target_port = args.target.split(':')
        target_host.split('.')[3]
        target_port = int(target_port)
        dct['target_host'] = target_host
        dct['target_port'] = target_port
    except Exception as e:
        print("无效参数：-T")
        sys.exit(1)
    # db
    db_list = args.database.split(':')
    if len(db_list) == 2:
        dct['source_db'] = db_list[0]
        dct['target_db'] = db_list[1]
    else:
        dct['source_db'] = db_list[0]
        dct['target_db'] = db_list[0]

    # table
    tb_list = args.table.split(':')
    if len(tb_list) == 2:
        dct['source_table'] = tb_list[0]
        dct['target_table'] = tb_list[1]
    else:
        dct['source_table'] = tb_list[0]
        dct['target_table'] = tb_list[0]

    dct['where'] = args.where
    dct['charset'] = args.charset
    dct['user'] = args.user
    dct['password'] = args.password
    dct['mode'] = args.mode
    dct['repeat'] = args.repeat

    return dct


class ArchivePartTable:
    def __init__(self, conf):
        self.source_host = conf['source_host']
        self.source_port = conf['source_port']
        self.source_db = conf['source_db']
        self.source_table = conf['source_table']
        self.target_host = conf['target_host']
        self.target_port = conf['target_port']
        self.target_db = conf['target_db']
        self.target_table = conf['target_table']
        self.user = conf['user']
        self.password = conf['password']
        self.charset = conf['charset']
        self.where = expr_to_date(conf['where'])
        self.mode = expr_to_date(conf['mode'])
        self.source_conf = {"host": self.source_host, "port": self.source_port, "db": self.source_db, 'user': self.user,
                            "password": self.password}
        self.target_conf = {"host": self.target_host, "port": self.target_port, "db": self.target_db, 'user': self.user,
                            "password": self.password}
        self.oldest_partition = None
        self.precheck_result = False
        self.status = 'begin'

    def __str__(self):
        return str(self.__dict__)

    def get_column_type(self, column_name):
        "获取字段类型"
        sql = """select data_type from information_schema.COLUMNS where table_schema='{}' and table_name='{}' and column_name='{}'""".format(
            self.source_db, self.source_table, column_name)
        try:
            conn = util.mysql(self.source_conf)
            res = conn.query(sql)
            data_type = res[0]['data_type']
            conn.close()
            return data_type
        except Exception as e:
            logging.info("获取字段类型报错: ", e)
            sys.exit(1)

    def get_oldest_partition(self):
        "获取最旧的分区信息"
        sql = """select table_schema db,table_name tb,partition_name pname, partition_method method,partition_expression partition_by_column, partition_description less_than_value from information_schema.partitions 
        where table_schema='{}' and table_name='{}' order by less_than_value limit 1""".format(self.source_db,
                                                                                               self.source_table)
        conn = util.mysql(self.source_conf)
        res = conn.query(sql)
        conn.close()
        if len(res) == 1:
            self.oldest_partition_info = res[0]
        else:
            logging.info("没有找到源表的分区信息,请检查表是否存在或是否是分区表")
            sys.exit(1)

    def precheck(self):
        "预检查"
        self.status = 'precheck'
        logging.info("即将归档的分区信息：{}".format(self.oldest_partition_info))
        if self.oldest_partition_info['method'] == 'RANGE':
            self.partition_by_column = self.oldest_partition_info['partition_by_column']
            data_type = self.get_column_type(self.partition_by_column)
            if data_type == 'int':
                self.count_sql = """select count(*) from {}""".format(self.oldest_partition_info['tb'])
                self.partition_subsql = """ partition({})""".format(self.oldest_partition_info['pname'])
                self.drop_sql = "alter table {} drop partition {}".format(self.oldest_partition_info['tb'],
                                                                          self.oldest_partition_info['pname'])
                self.less_than_value = self.oldest_partition_info['less_than_value']

                _where = self.where.lower()
                _where = _where.replace(self.partition_by_column.lower(), "")
                _where = _where.replace("<", "")
                _sql = "select {} <{}".format(self.less_than_value, _where)
                # print(_sql)
                try:
                    conn = util.mysql(self.source_conf, mode='list')
                    _check_less_than_value = conn.query(_sql)[0][0]
                    conn.close()
                    if _check_less_than_value == 1:
                        self.precheck_result = True
                    else:
                        logging.info("less_than_value:{} 大于 {}".format(self.less_than_value, self.where))
                        logging.info("预检不通过，退出")
                        sys.exit(0)
                except Exception as e:
                    logging.info("判断where参数异常：{}".format(self.where))
                    logging.info(e)
                    sys.exit(1)
            else:
                logging.info("不支持分区键的字段类型")
                sys.exit(1)
        else:
            logging.info("不支持该分区类型：{}".format(self.oldest_partition_info['method']))
            sys.exit(1)

    def archive_partition(self):
        "调用pt-archiver"
        ts = time.time()
        _where = "{}<{}".format(self.partition_by_column, self.less_than_value)
        subcmd1 = "--source A={},h={},P={},u={},p={},D={},t={}".format(self.charset, self.source_host, self.source_port,
                                                                       self.user, self.password, self.source_db,
                                                                       self.source_table)
        subcmd2 = "--dest A={},h={},P={},u={},p={},D={},t={}".format(self.charset, self.target_host, self.target_port,
                                                                     self.user, self.password, self.target_db,
                                                                     self.target_table)
        if self.mode == "no-copy":
            logging.info("mode：{},不拷贝数据".format(self.mode))
        else:
            if self.mode == "slow-copy":
                logging.info("mode={},启用慢拷贝模式(兼容性高)".format(self.mode))
                cmd = 'pt-archiver {} {} --progress=1000000 --statistics --txn-size=1000 --no-delete --charset={} --check-charset --where "{}"'.format(
                    subcmd1, subcmd2, self.charset, _where)
            elif self.mode == "slow-replace":
                logging.info("mode={},启用慢替换模式(兼容性高)".format(self.mode))
                cmd = 'pt-archiver {} {} --progress=1000000 --statistics --replace --txn-size=1000 --no-delete --charset={} --check-charset --where "{}"'.format(
                    subcmd1, subcmd2, self.charset, _where)
            else:
                cmd = 'pt-archiver {} {} --progress=1000000 --statistics --bulk-insert --limit=1000 --commit-each --no-delete --charset={} --check-charset --where "{}"'.format(
                    subcmd1, subcmd2, self.charset, _where)
            logging.info("开始执行：{}".format(cmd))
            output = util.run_command_once_output(cmd)
            print(output)
            seconds = int(time.time() - ts)
            logging.info("执行结束,耗时{}s".format(seconds))

    def drop_partition(self):
        "核对数据"
        self.status = 'drop'
        sql = self.count_sql + self.partition_subsql
        logging.info(sql)
        logging.info("计算source端行数")
        conn = util.mysql(self.source_conf, mode='list')
        self.source_rowcnt = conn.query(sql)[0][0]
        conn.close()
        logging.info("source端行数:{}".format(self.source_rowcnt))

        logging.info("计算target端行数")
        conn = util.mysql(self.target_conf, mode='list')
        self.target_rowcnt = conn.query(sql)[0][0]
        conn.close()
        logging.info("target端行数:{}".format(self.target_rowcnt))

        if self.source_rowcnt == self.target_rowcnt:
            logging.info("源表和归档表行数一致，检查通过")
            logging.info("删除表分区: " + self.drop_sql)
            try:
                conn = util.mysql(self.source_conf, mode='list')
                # conn.execute("set sql_log_bin=0")
                self.source_rowcnt = conn.execute(self.drop_sql)
                conn.close()
                logging.info("删除表分区执行成功")
                self.status = 'done & ok'
            except Exception as e:
                logging.info("删除表分区执行失败: {}".format(e))
                sys.exit(1)
        else:
            logging.info("源表和归档表行数不一致，检查不通过")
            sys.exit(1)

    def run(self):
        self.get_oldest_partition()
        self.precheck()
        if self.precheck_result:
            self.archive_partition()
            self.drop_partition()


# main
if __name__ == "__main__":
    set_log_level()
    args = get_args()
    logging.info(args)

    while True:
        o = ArchivePartTable(args)
        o.run()
        if not args['repeat']:
            break
        if o.status != 'done & ok':
            break
