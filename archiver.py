#!/bin/python3
# -*- encoding: utf-8 -*-
"""
####################################################################################################
#  Name        :  archiver.py
#  Author      :  Elison
#  Email       :  Ly99@qq.com
#  Description :  mysql归档
#  Updates     :
#      Version     When            What
#      --------    -----------     -----------------------------------------------------------------
#      v1.0        2022-05-13
#      v1.1        2022-05-30      增加archive-slow、archive-to-file、delete模式
#      v1.2        2022-06-18      增加waiting状态，解决重复执行的bug
#      v1.3        2022-06-20      增加TODAY表达式
#      v1.3.1      2022-07-03      修复class mysql bug
#      v1.3.2      2022-08-29      修复is_during_time_window函数 bug
#      v1.3.3      2022-09-13      增加连接configdb重试机制：失败时重试3次
#      v1.3.4      2022-10-27      支持设置归档时间间隔功能
#      v1.3.5      2022-12-02      增加archive-slow-replace、archive-partition、archive-partition-slow模式
#      v1.3.6      2022-12-30      增加archive-no-ascend模式
####################################################################################################
"""

import os, sys
import time, datetime
import re
import logging
import threading
import queue
import signal
import util
import settings


def expr_to_date(expr):
    "表达式转日期"
    new_expr = expr
    try:
        today = datetime.datetime.now()
        regex = re.compile('{{.*?}}', re.S)
        res = regex.findall(expr)
        for i in res:
            delta_str = i.replace("{{", "").replace("}}", "").replace("TODAY", "0").replace("today", "0")
            delta_num = eval(delta_str)
            pastday = today + datetime.timedelta(days=delta_num, hours=0, minutes=0)
            new_expr = new_expr.replace(i, pastday.strftime("'%Y-%m-%d 00:00:00'"))
        return new_expr
    except Exception:
        return expr


def send_msg_to_wxwork(msg_title, msg_content):
    "发送消息到企微机器人"
    util.send_msg_to_wxwork(settings.WXWORK_WEBHOOK, msg_title, msg_content)


def set_log_level(level='info'):
    "设置日志等级"
    if level == 'debug':
        lv = logging.DEBUG
    else:
        lv = logging.INFO
    logging.basicConfig(stream=sys.stdout, level=lv,
                        format='[%(asctime)s.%(msecs)d] [%(levelname)s] %(funcName)s: %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')


def exit_handler(signum, frame):
    "处理退出信号"
    global PRODUCER_FINISH
    PRODUCER_FINISH = True
    logging.info('收到停止信号，程序准备退出')


def get_configdb_conn():
    "创建配置库连接"
    for i in range(3):
        try:
            conn = util.mysql(settings.CONFIG_DB)
            return conn
        except Exception as e:
            logging.warning('连接configdb报错：{}'.format(e))
            time.sleep(1)


def get_archive_config(conn):
    "获取归档配置"
    sql = "select * from archive_config where is_deleted=0"
    res = conn.query(sql)
    return res


def get_archive_tasks(conn):
    "获取归档任务"
    sql = "select * from archive_tasks where exec_status in ('initial','waiting timeout') and sys_ctime>=curdate() order by priority desc"
    res = conn.query(expr_to_date(sql))
    return res


def get_failed_tasks():
    "获取当天运行失败的作业"
    sql = "select id,exec_status from archive_tasks where sys_ctime>=curdate() and exec_status<>'done & ok'"
    conn = get_configdb_conn()
    res = conn.query(expr_to_date(sql))
    conn.close()
    return res


def send_exec_result():
    "发送执行报告"
    rows = []
    res = None
    try:
        res = get_failed_tasks()
        title = '{}执行报告'.format(settings.NAME)
        if len(res) > 0:
            text = '有{}个任务执行失败:\n'.format(len(res))
            for i in res:
                row = "task_id: {} , exec_status: {}".format(i['id'], i['exec_status'])
                rows.append(row)
            text += "\n".join(rows)
        else:
            text = '所有任务执行成功'
        send_msg_to_wxwork(title, text)
    except Exception:
        logging.error(res, exc_info=True)


def is_during_time_window(time_window_str):
    "是否在执行时间窗口"
    now_num = int(time.strftime('%H%M', time.localtime()))
    try:
        time_window_list = time_window_str.split(',')
        for i in time_window_list:
            start_time, end_time = i.split('-')
            start_time_num = int(start_time.replace(':', ''))
            end_time_num = int(end_time.replace(':', ''))
            if start_time_num <= now_num and now_num <= end_time_num:
                return 1
            else:
                return 0
    except Exception:
        return -1  # "time_window格式错误"


class ArchiveConfig:

    def __init__(self, conf):
        self.id = conf['id']
        self.source_host = conf['source_host']
        self.source_port = conf['source_port']
        self.user = settings.ARCHIVE_USER
        self.password = settings.ARCHIVE_PASSWORD
        self.source_db = conf['source_db']
        self.source_table = conf['source_table']
        self.dest_host = conf['dest_host']
        self.dest_port = conf['dest_port']
        self.dest_db = conf['dest_db']
        self.dest_table = conf['dest_table']
        self.archive_mode = conf['archive_mode']
        self.charset = conf['charset']
        self.interval_day = conf['interval_day']
        self.archive_condition = conf['archive_condition']
        self.exec_time_window = conf['exec_time_window']
        self.priority = conf['priority']
        self.exec_status = 'initial'
        self.archive_cmd_list = []

    def __str__(self):
        return str(self.__dict__)

    def is_need_run(self, conn):
        "判断是否符合满足interval天数"
        sql = "select count(*) cnt from archive_tasks where config_id={} and exec_status in ('done & ok','running') and exec_start>=date_add(curdate(),interval {} day)".format(
            self.id, 1 - self.interval_day)
        res = conn.query(sql)
        cnt = res[0]['cnt']
        if cnt == 0:
            return True
        else:
            logging.info("不满足间隔天数:[ id:{},interval_day:{} ]".format(self.id, self.interval_day))
            return False

    def generate_cmds(self):
        "生成归档命令"
        subcmd1 = "--source A={},h={},P={},u={},p={},D={},t={}".format(self.charset, self.source_host, self.source_port,
                                                                       self.user, self.password, self.source_db,
                                                                       self.source_table)
        subcmd2 = "--dest A={},h={},P={},u={},p={},D={},t={}".format(self.charset, self.dest_host, self.dest_port,
                                                                     self.user, self.password, self.dest_db,
                                                                     self.dest_table)

        # 转换日期
        self.archive_condition = expr_to_date(self.archive_condition)

        if self.archive_mode == 'archive':
            cmd = 'pt-archiver {} {} --progress=10000 --statistics --bulk-insert --limit=1000 --bulk-delete --commit-each --charset=utf8 --check-charset'.format(
                subcmd1, subcmd2)
            archive_cmd = '{} --where="{}"'.format(cmd, self.archive_condition)
            self.archive_cmd_list.append(archive_cmd)
        elif self.archive_mode == 'archive-no-ascend':
            cmd = 'pt-archiver {} {} --progress=10000 --statistics --bulk-insert --limit=1000 --bulk-delete --commit-each --charset=utf8 --check-charset --no-ascend'.format(
                subcmd1, subcmd2)
            archive_cmd = '{} --where="{}"'.format(cmd, self.archive_condition)
            self.archive_cmd_list.append(archive_cmd)
        elif self.archive_mode == 'archive-slow':
            cmd = 'pt-archiver {} {} --progress=10000 --statistics --bulk-delete --commit-each --limit=1000 --charset=utf8 --check-charset'.format(
                subcmd1, subcmd2)
            archive_cmd = '{} --where="{}"'.format(cmd, self.archive_condition)
            self.archive_cmd_list.append(archive_cmd)
        elif self.archive_mode == 'archive-slow-replace':
            cmd = 'pt-archiver {} {} --progress=10000 --statistics --replace --bulk-delete --commit-each --limit=1000 --charset=utf8 --check-charset'.format(
                subcmd1, subcmd2)
            archive_cmd = '{} --where="{}"'.format(cmd, self.archive_condition)
            self.archive_cmd_list.append(archive_cmd)
        elif self.archive_mode == 'delete':
            cmd = 'pt-archiver {} --progress=10000 --statistics --txn-size=1000 --purge --bulk-delete --limit=1000 --charset=utf8 --check-charset'.format(
                subcmd1)
            archive_cmd = '{} --where="{}"'.format(cmd, self.archive_condition)
            self.archive_cmd_list.append(archive_cmd)
        elif self.archive_mode == 'archive-to-file':
            dirname = "archive_data/{}_{}/{}.{}".format(self.source_host, self.source_port, self.source_db,
                                                        self.source_table)
            filename = "{}/%Y%m%d_%H.dat".format(dirname)
            if not os.path.exists(dirname):
                os.makedirs(dirname)
            cmd = 'pt-archiver {} --progress=10000 --statistics --txn-size=1000 --file={} --bulk-delete --limit=1000 --charset=utf8 --check-charset'.format(
                subcmd1, filename)
            archive_cmd = '{} --where="{}"'.format(cmd, self.archive_condition)
            self.archive_cmd_list.append(archive_cmd)
        elif self.archive_mode == 'archive-partition':
            cmd = 'python3 archive_partition_table.py -S {}:{} -T {}:{} -d {}:{} -t {}:{} -c {}'.format(
                self.source_host, self.source_port, self.dest_host, self.dest_port, self.source_db, self.dest_db,
                self.source_table, self.dest_table, self.charset)
            cmd += " -m copy"
            archive_cmd = '{} --where="{}"'.format(cmd, self.archive_condition)
            self.archive_cmd_list.append(archive_cmd)
        elif self.archive_mode == 'archive-partition-slow-copy':
            cmd = 'python3 archive_partition_table.py -S {}:{} -T {}:{} -d {}:{} -t {}:{} -c {}'.format(
                self.source_host, self.source_port, self.dest_host, self.dest_port, self.source_db, self.dest_db,
                self.source_table, self.dest_table, self.charset)
            cmd += " -m slow-copy"
            archive_cmd = '{} --where="{}"'.format(cmd, self.archive_condition)
            self.archive_cmd_list.append(archive_cmd)
        elif self.archive_mode == 'archive-partition-slow-replace':
            cmd = 'python3 archive_partition_table.py -S {}:{} -T {}:{} -d {}:{} -t {}:{} -c {}'.format(
                self.source_host, self.source_port, self.dest_host, self.dest_port, self.source_db, self.dest_db,
                self.source_table, self.dest_table, self.charset)
            cmd += " -m slow-replace"
            archive_cmd = '{} --where="{}"'.format(cmd, self.archive_condition)
            self.archive_cmd_list.append(archive_cmd)
        else:
            logging.error("archive_mode参数错误：[ id:{},archive_mode:{} ]".format(self.id, self.archive_mode))

    def save_tasks(self, conn):
        "保存任务"
        table_name = 'archive_tasks'
        fieldname_list = ['config_id', 'source_host', 'source_port', 'source_db', 'source_table', 'dest_host',
                          'dest_port', 'dest_db', 'dest_table', 'archive_mode', 'exec_time_window', 'priority',
                          'exec_status', 'archive_cmd']
        rows = []
        for cmd in self.archive_cmd_list:
            row = [self.id, self.source_host, self.source_port, self.source_db, self.source_table,
                   self.dest_host, self.dest_port, self.dest_db, self.dest_table, self.archive_mode,
                   self.exec_time_window, self.priority,
                   self.exec_status, cmd]
            rows.append(row)
        conn.batch_insert(table_name, fieldname_list, rows)

    def start(self, conn):
        "生成任务"
        if self.is_need_run(conn):
            self.generate_cmds()
            self.save_tasks(conn)


class ArchiveTask:

    def __init__(self, conf):
        self.id = conf['id']
        self.user = settings.ARCHIVE_USER
        self.password = settings.ARCHIVE_PASSWORD
        self.source_host = conf['source_host']
        self.source_port = conf['source_port']
        self.source_db = conf['source_db']
        self.source_table = conf['source_table']
        self.archive_mode = conf['archive_mode']
        self.dest_host = conf['dest_host']
        self.dest_port = conf['dest_port']
        self.dest_db = conf['dest_db']
        self.dest_table = conf['dest_table']
        self.archive_cmd = conf['archive_cmd']
        self.exec_time_window = conf['exec_time_window']
        self.exec_seconds = 0
        self.exec_log = ""
        self.logfile = "logs/{}.log".format(self.id)

    def __str__(self):
        return str(self.__dict__)

    def log_task_begin(self):
        "记录任务开始"
        conn = get_configdb_conn()
        sql = "update archive_tasks set exec_status='running',exec_start=now() where id={0}".format(self.id)
        conn.execute(sql)
        conn.close()

    def log_task_status(self):
        "记录任务状态"
        conn = get_configdb_conn()
        sql = "update archive_tasks set exec_status='{1}',exec_seconds={2},exec_end=now() where id={0}".format(
            self.id, self.exec_status, self.exec_seconds)
        conn.execute(sql)
        conn.close()

    def update_task_log(self):
        "更新任务日志"
        conn = get_configdb_conn()
        sql = "update archive_tasks set exec_log='{1}' where id={0}".format(self.id, self.exec_log.replace("'", '"'))
        conn.execute(sql)
        conn.close()

    @staticmethod
    def get_table_fields(conf, table):
        "获取表结构信息"
        sql = "desc {}".format(table)
        conn = util.mysql(conf)
        rows = conn.query(sql)
        conn.close()
        return rows

    def check(self):
        "检查"
        retcode = 0

        # 检查执行时间窗口
        check_code = is_during_time_window(self.exec_time_window)
        if check_code == 0:
            self.exec_status = "waiting timeout"
            self.exec_log = "程序并发设置过低，等待超时。"
            return retcode
        elif check_code < 0:
            self.exec_status = "check failed"
            self.exec_log = "time_window格式错误:{}".format(self.exec_time_window)
            logging.error("time_window格式错误:{}".format(self.exec_time_window))
            return retcode

        # 检查归档库和表是否存在或列是否一致
        if self.archive_mode in ['archive', 'archive-slow']:
            source_conf = {'host': self.source_host, 'port': self.source_port, 'db': self.source_db, 'user': self.user,
                           'password': self.password}
            dest_conf = {'host': self.dest_host, 'port': self.dest_port, 'db': self.dest_db, 'user': self.user,
                         'password': self.password}
            try:
                source_tb_fields = self.get_table_fields(source_conf, self.source_table)
                dest_tb_fields = self.get_table_fields(dest_conf, self.dest_table)
                source_fieldnames = [i['Field'] for i in source_tb_fields]
                dest_fieldnames = [i['Field'] for i in dest_tb_fields]
                field_not_exist_in_dest = [i for i in source_fieldnames if i not in dest_fieldnames]
                if field_not_exist_in_dest:
                    self.exec_status = "check failed"
                    self.exec_log = "目标表字段不存在：{}".format(','.join(field_not_exist_in_dest))
                elif len(source_fieldnames) != len(dest_fieldnames):
                    self.exec_status = "check failed"
                    self.exec_log = "字段数量不一致"
                else:
                    self.exec_status = "check passed"
                    self.exec_log = "检查通过"
                    retcode = 1
            except Exception as e:
                self.exec_status = "check failed"
                self.exec_log = str(e)
                logging.error(self, exc_info=True)
        else:
            retcode = 1
        return retcode

    def start(self):
        "开始任务"
        logging.info("开始执行：[task_id:{}]".format(self.id))
        ts = time.time()
        self.exec_status = 'running'
        self.log_task_begin()
        if self.check() == 1:
            logging.info("检查通过：[task_id:{}]".format(self.id))
            self.exec_log = ""
            exit_code = util.run_command(self.archive_cmd, self.logfile)
            if exit_code == 0:
                self.exec_status = 'done & ok'
            else:
                self.exec_status = 'done & error:[exit_code={}]'.format(exit_code)
            with open(self.logfile, 'r') as f:
                self.exec_log = f.read()
            self.update_task_log()
            self.exec_seconds = int(time.time() - ts)
            self.log_task_status()
            logging.info("执行结束：[task_id:{}]，耗时：{}s".format(self.id, self.exec_seconds))
        else:
            # 检查失败
            logging.info("检查失败：[task_id:{0},exec_status:{1}] ".format(self.id, self.exec_status))
            self.exec_seconds = int(time.time() - ts)
            self.log_task_status()
            self.update_task_log()


def generate_tasks():
    "生成归档任务"
    conf_list = None
    conn = get_configdb_conn()
    conf_list = get_archive_config(conn)
    for conf in conf_list:
        try:
            o = ArchiveConfig(conf)
            o.start(conn)
        except Exception:
            logging.error(conf, exc_info=True)
    conn.close()


def generate_task_job():
    "每天0:00生成归档任务"
    while True:
        tm_sec = time.localtime().tm_sec
        time.sleep(60 - tm_sec)  # 下一分钟0秒执行
        if PRODUCER_FINISH:
            break
        now_time = time.strftime('%H:%M', time.localtime())
        if now_time == "00:00":  # 每天0:00执行
            logging.info('生成归档任务')
            try:
                generate_tasks()
            except Exception:
                logging.error(exc_info=True)
        elif now_time == "08:00" and hasattr(settings, 'WXWORK_WEBHOOK'):
            logging.info('发送归档报告')
            send_exec_result()
        else:
            logging.debug('sleep 60 seconds')


def produce_job():
    "生产作业"
    global JOB_QUEUE
    global PRODUCER_FINISH
    task_list = []
    while True:
        # 6秒检测一次
        for i in range(10):
            time.sleep(6)
            if PRODUCER_FINISH:
                return 1
        try:
            conn = get_configdb_conn()
            task_list = get_archive_tasks(conn)
            # 过滤不在执行时间窗口的
            to_exec_task_list = [i for i in task_list if is_during_time_window(i['exec_time_window']) == 1]
            logging.info('有{}个任务推送到执行队列'.format(len(to_exec_task_list)))
            for task_conf in to_exec_task_list:
                sql = "update archive_tasks set exec_status='waiting' where id={0}".format(task_conf['id'])
                conn.execute(sql)
                obj = ArchiveTask(task_conf)
                JOB_QUEUE.put(obj)
            conn.close()
        except Exception:
            logging.error(task_list, exc_info=True)


def consume_job():
    "消费作业"
    global STOP_TOKEN
    global JOB_QUEUE
    obj = None
    while True:
        try:
            obj = JOB_QUEUE.get()  # 取实例信息
            if obj == STOP_TOKEN:
                break
            obj.start()
        except Exception:
            logging.error(obj, exc_info=True)


# main
if __name__ == "__main__":
    # 设置工作路径
    dirname, filename = (os.path.split(os.path.realpath(__file__)))
    os.chdir(dirname)
    if not os.path.exists('logs'):
        os.makedirs('logs')

    # 设置日志格式
    set_log_level(settings.LOGGING_LEVEL)

    PARALLEL = settings.PARALLEL
    PRODUCER_FINISH = False
    STOP_TOKEN = 'stop!!!'  # 停止信号
    JOB_QUEUE = queue.Queue(maxsize=1000)

    logging.info('【程序开始启动】')

    # 开启generate_task_job线程
    task_generator = threading.Thread(name='TaskGenerator', target=generate_task_job, args=())  # 创建线程
    task_generator.setDaemon(True)  # 设置为守护线程
    task_generator.start()  # 启动线程
    logging.info('{0}线程已启动！'.format(task_generator.name))

    # 开启生产线程
    producer = threading.Thread(name='Producer', target=produce_job, args=())  # 创建线程
    producer.setDaemon(True)  # 设置为守护线程
    producer.start()  # 启动线程
    logging.info('{0}线程已启动！'.format(producer.name))

    # 开启消费线程
    consumers = [threading.Thread(name='Consumer-' + str(i), target=consume_job, args=()) for i in range(PARALLEL)]
    [i.setDaemon(True) for i in consumers]  # 设置为守护线程
    [i.start() for i in consumers]  # 启动线程
    [logging.info('{0}线程已启动！'.format(i.name)) for i in consumers]

    # 接收停止信号
    signal.signal(signal.SIGTERM, exit_handler)

    while True:
        time.sleep(2)
        if PRODUCER_FINISH:
            break
        # 检测子线程是否正常
        if not producer.is_alive():
            logging.error('{0}线程异常退出!'.format(producer.name))
        for i in consumers:
            if not i.is_alive():
                logging.error('{0}线程异常退出!'.format(i.name))

    # 回收task_generator线程
    # task_generator.join()
    # logging.info('{0}线程已退出！'.format(task_generator.name))

    # 回收producer线程
    producer.join()
    logging.info('{0}线程已退出！'.format(producer.name))

    # 回收consumer线程
    [JOB_QUEUE.put(STOP_TOKEN) for i in range(PARALLEL * 2)]  # 发送停止信号
    for i in consumers:
        i.join()
        logging.info('{0}线程已退出！'.format(i.name))

    # 程序退出
    logging.info('【程序退出成功】')
