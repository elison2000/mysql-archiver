--settings.py中的用户自行创建
create database mysql_archiver;
drop table if exists archive_config;
CREATE TABLE `archive_config` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT 'id',
  `source_host` varchar(64) NOT NULL COMMENT '源服务器',
  `source_port` int(11) NOT NULL COMMENT '源服务器端口',
  `source_db` varchar(64) NOT NULL COMMENT '源数据库schema',
  `source_table` varchar(128) NOT NULL COMMENT '源数据库表',
  `dest_host` varchar(64) NOT NULL COMMENT '目标服务器',
  `dest_port` int(11) NOT NULL DEFAULT '3306' COMMENT '目标服务器端口',
  `dest_db` varchar(64) NOT NULL DEFAULT '' COMMENT '目标数据库schema',
  `dest_table` varchar(128) NOT NULL DEFAULT '' COMMENT '目标数据库表',
  `archive_mode` varchar(20) NOT NULL DEFAULT 'archive' COMMENT '归档模式：archive（归档），archive-slow(慢模式，兼容性高),delete(只删除不归档)，archive-to-file(归档到文件)',
  `charset` varchar(20) NOT NULL DEFAULT 'utf8mb4' COMMENT '字符集',
  `archive_condition` varchar(1000) NOT NULL DEFAULT '' COMMENT '归档条件',
  `exec_time_window` varchar(1000) NOT NULL DEFAULT '00:00-06:00' COMMENT '执行时间窗口，如：00:00-06:00,22:00-24:00',
  `priority` tinyint(4) DEFAULT '1' COMMENT '优化级，数值越高，在执行时间窗口的有多个任务时，优先执行',
  `sys_ctime` datetime DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `sys_utime` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  `is_deleted` tinyint(4) DEFAULT '0' COMMENT '是否已删除',
  `remark` varchar(200) DEFAULT NULL COMMENT '备注信息',
  PRIMARY KEY (`id`),
  KEY `idx_source_db_source_table` (`source_db`,`source_table`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COMMENT='归档配置表';

INSERT INTO dba_tools.archive_config
(id, source_host, source_port, source_db, source_table, dest_host, dest_port, dest_db, dest_table, archive_mode,
 charset, archive_condition, exec_time_window, priority, sys_ctime, sys_utime, is_deleted, remark)
VALUES (1, '10.177.13.205', 3306, 'datacube', 'dc_log_oa_site_stocks', '10.177.13.197', 3310, 'datacube',
        'dc_log_oa_site_stocks', '默认', 'utf8mb4', 'add_time<=1615705200', '00:00-06:00', 1, '2022-05-25 07:15:31',
        '2022-05-25 08:34:59', 0, NULL);



drop table if exists archive_tasks;
CREATE TABLE `archive_tasks`
(
    `id`               int(11) NOT NULL AUTO_INCREMENT COMMENT 'id',
    `source_host`      varchar(64)  NOT NULL COMMENT '源服务器',
    `source_port`      int(11) NOT NULL COMMENT '源服务器端口',
    `source_db`        varchar(64)  NOT NULL COMMENT '源数据库schema',
    `source_table`     varchar(128) NOT NULL COMMENT '源数据库表',
    `dest_host`        varchar(64)  NOT NULL COMMENT '目标服务器',
    `dest_port`        int(11) NOT NULL COMMENT '目标服务器端口',
    `dest_db`          varchar(64)   DEFAULT NULL COMMENT '目标数据库schema',
    `dest_table`       varchar(128)  DEFAULT NULL COMMENT '目标数据库表',
    `archive_mode`     varchar(20)   DEFAULT 'archive' COMMENT '归档模式：archive（归档），archive-slow(慢模式，兼容性高),delete(只删除不归档)，archive-to-file(归档到文件)',
    `exec_time_window` varchar(1000) DEFAULT NULL COMMENT '执行时间窗口',
    `priority`         tinyint(4) DEFAULT '1' COMMENT '优化级，数值越高，在执行时间窗口的有多个任务时，优先执行',
    `exec_status`      varchar(100)  DEFAULT 'initial' COMMENT '运行的状态，initial:初始状态，running:执行中，check failed:检查不通过，wait timeout:等待超时，done:已执行',
    `exec_start`       datetime      DEFAULT NULL COMMENT '归档开始时间',
    `exec_end`         datetime      DEFAULT NULL COMMENT '归档结束时间',
    `exec_seconds`     int(11) DEFAULT NULL COMMENT '执行时间（秒）',
    `archive_cmd`      varchar(2000) DEFAULT NULL COMMENT '归档命令',
    `exec_log`         longtext COMMENT '执行日志',
    `sys_utime`        datetime      DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
    PRIMARY KEY (`id`),
    KEY                `idx_source_db_table` (`source_db`,`source_table`,`exec_start`),
    KEY                `idx_source_host_port` (`source_host`,`source_port`,`exec_start`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='归档任务表';