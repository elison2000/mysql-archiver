#!/bin/python3
# -*- encoding: utf-8 -*-


import archiver


# 测试 get_archive_config
# conn = archiver.get_configdb_conn()
# res = archiver.get_archive_config(conn)
# print(res)

# 测试 get_archive_tasks
# conn = archiver.get_configdb_conn()
# res = archiver.get_archive_tasks(conn)
# print(res)

# 测试 is_during_time_window
# time_win_str = '00:00-21:00'
# res = archiver.is_during_time_window(time_win_str)
# print(res)


def test_generate_cmds(id=1):
    "生成归档任务"
    conn = archiver.get_configdb_conn(mode='dict')
    res = archiver.get_archive_config(conn)
    res = [i for i in res if i['id'] == id]
    conf = res[0]
    print(conf)
    o = archiver.ArchiveConfig(conf)
    o.generate_cmds()
    print(o.archive_cmd_list)
    # o.save_tasks(conn)


def test_exec_task(id=1):
    "测试执行"
    conn = archiver.get_configdb_conn(mode='dict')
    res = archiver.get_archive_tasks(conn)
    res = [i for i in res if i['id'] == id]
    conf = res[0]
    print(conf)
    t = archiver.ArchiveTask(conf)
    print(t.check())
    print(t.exec_log)
    # t.start()
    print(t)

# main
# id = 1
# test_generate_cmds(id)  # 测试 ArchiveConfig 生成任务
# test_exec_task(id)  # 测试 ArchiveTask执行
# archiver.send_exec_result()
