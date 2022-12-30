### mysql-archiver程序说明

用于搭建归档平台，集调度、告警功能，支持多种归档方式。

### mysql-archiver文件说明

```
admin.sh        --启动、关闭脚本
archiver.py     --主程序
init.sql        --建表
settings.py     --配置文件
debug.py        --测试
util.py         --公共函数
logs            --任务运行的实时日志（行缓冲），任务执行结束，才会把日志保存到表中
archive_data    --当归档模式为archive-to-file时，归档数据存放到该目录
```



### 部署说明

#### 1、下载安装Percona-Server版本，带tokudb插件（不是Percona Distribution for MySQL）

#### 2、安装tokudb存储引擎（高压缩率），也可以使用innodb存储引擎(不压缩，不建议)。

安装文档参考 https://www.percona.com/doc/percona-server/5.7/tokudb/tokudb_installation.html

#### 3、启动实例，执行init.sql，创建库、表、用户。

```
archive_config --归档配置表
archive_tasks  --归档任务表，根据配置表每天生成一条归档任务
```

#### 4、在需要归档的实例创建归档用户dba_archive_user

#### 5、修改settings.py的配置信息，修改数据库IP、端口、用户、密码等。



### 脚本启动与停止

./admin.sh

```
Usage: ./admin.sh {start|stop|status}
```



### 脚本补充说明

1、程序每天凌晨00:00 根据 archive_config中信息生成待执行的任务插入archive_tasks表。
2、程序每分钟检测一次archive_tasks中未执行的任务，检测时间窗口符合后，推送到执行队列。
3、程序最大并发数：5（可配置），并发数达到最大时，执行队列中的任务会进入等待，等待结果存在2中情况：
  前置任务结束，开始执行。
  等待时间已经超出执行的时间窗口，等待下一个时间窗口调起。
4、执行结束后，可在archive_tasks查看执行日志，exec_status：执行状态，exec_log：执行日志。

```
归档命令默认参数：
--bulk-insert   批量插入、删除（效率高）
--limit=1000    和bulk-insert配合使用
--charset=utf8  和bulk-insert配合使用
```



### 归档模式

```
archive-slow：归档速度慢，兼容性高
delete：只删除不归档
archive-to-file：归档到文件
archive：默认方式，采用--bulk-insert --bulk-delete的方式归档，速度快
archive-slow-replace:重复行替换模式
archive-partition：分区表归档
archive-partition-slow：分区表归档慢模式
archive-no-ascend：禁用FORCE INDEX(`PRIMARY`)，不按主键顺序扫描，where列有索引时，速度快
```



archive：默认方式可能导致报错：DBD::mysql::st execute failed: Invalid utf8 character string: ... at /bin/pt-archiver line 6876.

原因是：pt-archiver的参数--charset只支持utf8,不支持utf8mb4，遇到utf8不兼容的特殊符号时，抛出bulk-insert异常

解决方案：
```
1、使用archive-slow模式归档
2、修改pt-archiver的代码，强制使用utf8mb4：
. ($got_charset ? "CHARACTER SET $got_charset" : "")改为. ($got_charset ? "CHARACTER SET utf8mb4" : "")
```



### 配置样例说明

实例172.31.100.171:3306上的tmdb.transfer_monitor_fee表归档到192.168.36.61:3306，归档表名和库名不变，归档时间为120天前的数据：

1、在172.31.100.171:3306 创建用户dba_archive_user，权限和密码见 数据库默认账号文件。

2、在192.168.36.61:3306创建用户dba_archive_user。（如果用户存在，可忽略）

3、在archive_config插入配置信息

```
INSERT INTO  archive_config
(id, source_host, source_port, source_db, source_table, dest_host, dest_port, dest_db, dest_table, archive_mode, charset, archive_condition, exec_time_window, priority, sys_ctime, sys_utime, is_deleted, remark)
VALUES(1, '10.177.13.205', 3306, 'datacube', 'dc_log_oa_site_stocks', '10.0.0.197', 3310, 'datacube', 'dc_log_oa_site_stocks', 'archive', 'utf8mb4', 'add_time<={{today - 30}}', '00:00-06:00', 1, '2022-05-25 07:15:31', '2022-05-27 09:05:43', 0, NULL);
```

4、第二天查看archive_tasks，是否自动生成调度任务。

5、待exec_time_window时间到了，检查archive_config.exec_status的执行状态。

运行的状态：

```
initial: 初始状态
running: 执行中
check failed: 检查不通过
waiting: 任务进入执行队列，执行顺序由archive_tasks.priority的值控制，并发数通过settings中的PARALLEL参数控制。
waiting timeout: 线程繁忙，等待超时。（无需处理，下一个时间窗口会自动调起）
done: 执行结束，有报错
done & ok : 执行成功
```

6、archive_condition条件说明
2种配置方式：
方式1：使用mysql函数生成日期范围
方式2：使用表达式生成日期范围

datetime类型:  

```
create_time < date_add(curdate(),interval -180 day)
create_time < {{ TODAY - 30*6 }} 
```

10位整数的unix_timestamp类型: 

```
create_ts < unix_timestamp(date_add(curdate(),interval -180 day))
create_ts < unix_timestamp({{ TODAY - 30*6 }})
```
