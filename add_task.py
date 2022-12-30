#!/bin/python3
# -*- encoding: utf-8 -*-
"""
####################################################################################################
#  Name        :  add_task.py
#  Author      :  Elison
#  Email       :  Ly99@qq.com
#  Description :  添加归档任务
#  Updates     :
#      Version     When            What
#      --------    -----------     -----------------------------------------------------------------
#      v1.0        2022-10-26
####################################################################################################
"""

import sys
import re
import argparse
import archiver
import settings
import util


def get_args():
    '获取参数'
    parser = argparse.ArgumentParser()
    parser.add_argument("-v", "--version", action='store_true', help="查看版本")
    parser.add_argument("-S", "--source", type=str, required=True, help="归档的表所在的实例IP和端口, 如: 10.0.0.201:3306")
    parser.add_argument("-d", "--database", type=str,
                        help="归档表的数据库名，如：orderdb或orderdb:orderdb_history(可以使用冒号分别指定源端和目标端db名)")
    parser.add_argument("-t", "--table", type=str, help="归档表的表名，如：orders或orders:orders_history(可以使用冒号分别指定源端和目标端table名)")
    parser.add_argument("-m", "--mode", type=str, default='archive',
                        choices=['archive', 'archive-slow', 'archive-slow-replace', 'delete', 'archive-to-file',
                                 'archive-partition', 'archive-partition-slow'],
                        help="核对模式，archive：速度快；archive-slow：速度慢，兼容性高；delete：只删除不归档")
    parser.add_argument("-w", "--where", type=str, required=True, help="归档条件")
    parser.add_argument("-i", "--interval", type=int, default=1, help="执行间隔天数,默认间隔1天")

    args = parser.parse_args()

    # 处理参数
    if args.version:
        print(__doc__)
        sys.exit()

    return args


def format_args(args):
    "格式化参数"

    dct = {}
    try:
        source_host, source_port = args.source.split(':')
        source_host.split('.')[3]
        int(source_port)
        dct['source_host'] = source_host
        dct['source_port'] = source_port
    except Exception as e:
        print("无效参数：-S")
        sys.exit()

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

    dct['target_host'] = settings.ARCHIVE_DB['host']
    dct['target_port'] = str(settings.ARCHIVE_DB['port'])
    dct['mode'] = args.mode
    dct['where'] = args.where
    dct['interval'] = args.interval
    return dct


def insert_task(dct):
    "插入任务"
    source_host = dct['source_host']
    source_port = dct['source_port']
    source_db = dct['source_db']
    source_table = dct['source_table']
    target_host = dct['target_host']
    target_port = dct['target_port']
    target_db = dct['target_db']
    target_table = dct['target_table']
    mode = dct['mode']
    charset = "utf8mb4"
    where = dct['where']
    interval = dct['interval']
    sql = """INSERT INTO archive_config(source_host,source_port,source_db,source_table,dest_host,dest_port,dest_db,dest_table,archive_mode,charset,archive_condition,interval_day) VALUES
('{0}',{1},'{2}','{3}','{4}',{5},'{6}','{7}','{8}','{9}','{10}',{11})""".format(source_host, source_port, source_db,
                                                                                source_table, target_host, target_port,
                                                                                target_db, target_table, mode, charset,
                                                                                where, interval)
    db = archiver.get_configdb_conn()
    cursor = db.conn.cursor()
    cursor.execute(sql)
    lastrowid = cursor.lastrowid
    db.conn.commit()
    db.close()
    return lastrowid


def get_source_table_ddl(dct):
    "获取表ddl"
    conf = {"host": dct['source_host'], "port": int(dct['source_port']), "db": dct['source_db'],
            'user': settings.ARCHIVE_USER, "password": settings.ARCHIVE_PASSWORD}
    sql = "show create table {}".format(dct['source_table'])
    conn = util.mysql(conf)
    res = conn.query(sql)
    conn.close()
    if len(res) == 0:
        print("source端不存在表：{}.{}".format(dct['source_db'], dct['source_table']))
        return False
    else:
        ddl = res[0]['Create Table']
        return ddl


def create_target_table(dct, ddl):
    "在归档库创建表"
    retcode = 0
    conf = {"host": dct['target_host'], "port": int(dct['target_port']), 'user': settings.ARCHIVE_USER,
            "password": settings.ARCHIVE_PASSWORD}
    new_ddl = re.sub("CREATE TABLE `\w+`", 'CREATE TABLE {}.{}'.format(dct['target_db'], dct['target_table']),
                     ddl)  # 改表名
    new_ddl = re.sub(" ENGINE=\w+ ", '', new_ddl)  # 改存储引擎
    new_ddl = re.sub(" ENGINE *= *\w+", '', new_ddl)  # 改存储引擎(有空格)

    conn = util.mysql(conf)
    print(new_ddl)
    choose = input("是否创建以上表结构[y\\n]:")
    if choose == 'y':
        res = conn.query("show databases")
        db_list = [i['Database'] for i in res]
        if dct['target_db'] not in db_list:
            print("target端创建数据库成功:{}".format(dct['target_db']))
            conn.execute("create database {} charset utf8mb4".format(dct['target_db']))
        res = conn.execute(new_ddl)
        print("target端创建表成功：{}.{}".format(dct['target_db'], dct['target_table']))
        retcode = 1
    else:
        print("取消创建表")
    conn.close()
    return retcode


if __name__ == "__main__":
    args = get_args()
    task_data = format_args(args)

    # 创建归档表
    ddl = get_source_table_ddl(task_data)
    retcode = create_target_table(task_data, ddl)

    # 插入config数据
    if retcode == 1:
        task_id = insert_task(task_data)
        print("表archive_config已插入配置: [id:{}]".format(task_id))
