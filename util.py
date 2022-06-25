#!/bin/python3
# -*- encoding: utf-8 -*-
# ---------------------------------------------- #
#  Name         :  util.py                       #
#  Author       :  Elison                        #
#  Email        :  Ly99@qq.com                   #
#  Description  :  公共函数                      #
#  Version      :  v1.0                          #
#  LastUpdated  :  2021-08-30                    #
# ---------------------------------------------- #

import time
import subprocess
import requests
import pymysql


def get_now():
    now = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
    return now


def send_msg_to_wxwork(webhook, msg_title, msg_content):
    "发送消息到企微机器人"
    headers = {"Content-Type": "text/plain"}
    text = '## <font color="warning">{0}</font>\n{1}'.format(
        msg_title, msg_content)
    data = {
        "msgtype": "markdown",
        "markdown": {
            "content": text
        }}
    response = requests.post(url=webhook, headers=headers, json=data, timeout=10)
    return response.json()


def run_command(command, logfile):
    "运行命令"
    with open(logfile, 'w') as f:
        p = subprocess.Popen(command, shell=True, stdout=f, stderr=subprocess.STDOUT, bufsize=1,
                             env={'LANG': 'en_US.UTF-8'})
        p.wait()
    return p.returncode


def run_command_realtime(command):
    "运行命令"
    # 2秒刷新一次标准输出
    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                               env={'LANG': 'en_US.UTF-8'})
    while True:
        line = process.stdout.readline()
        if line == b'' and process.poll() is not None:
            errmsg_list = process.stderr.readlines()
            if errmsg_list:
                errmsg_bytes = b''
                for _bytes in errmsg_list:
                    errmsg_bytes += _bytes
                errmsg = errmsg_bytes.decode('utf8')
            else:
                errmsg = '[OK]'
            yield errmsg
            break
        if line:
            try:
                text = line.decode('utf8')
                yield text
            except Exception as e:
                print(str(e))
                print(line)
        else:
            time.sleep(2)


class mysql:
    "mysql接口"

    def __init__(self, conf, mode='list'):
        if mode == 'dict':
            conf['connect_timeout'] = 2
            conf['cursorclass'] = pymysql.cursors.DictCursor
        self.conn = pymysql.connect(**conf)

    def query(self, sql):
        "查询"
        cur = self.conn.cursor()
        cur.execute(sql)
        res = cur.fetchall()
        cur.close()
        return res

    def execute(self, sql, row=None):
        "执行"
        cur = self.conn.cursor()
        if row:
            cur.execute(sql, row)
        else:
            cur.execute(sql)
        self.conn.commit()
        cur.close()
        return 1

    def batch_insert(self, table_name, fieldname_list, rows):
        "批量插入数据"
        cur = self.conn.cursor()
        value_text = ','.join(['%s' for i in fieldname_list])
        fieldname_text = ','.join(fieldname_list)
        sql = 'insert into {0}({1}) values({2})'.format(table_name, fieldname_text, value_text)
        cur.executemany(sql, rows)
        self.conn.commit()
        cur.close()

    def batch_replace(self, table_name, fieldname_list, rows):
        "批量替换数据"
        cur = self.conn.cursor()
        cur.execute("set session transaction isolation level READ COMMITTED")
        value_text = ','.join(['%s' for i in fieldname_list])
        fieldname_text = ','.join(fieldname_list)
        sql = 'replace into {0}({1}) values({2})'.format(table_name, fieldname_text, value_text)
        cur.executemany(sql, rows)
        self.conn.commit()
        cur.close()

    def close(self):
        "关闭数据库连接"
        self.conn.close()


# main
if __name__ == "__main__":
    cmd = 'ls -l'
    res = run_command(cmd)
    print(res)
