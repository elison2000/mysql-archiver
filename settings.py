CONFIG_DB = {'host': '10.0.0.200', 'port': 3306, 'db': 'mysql_archiver', 'user': 'mysql_archiver_rw', 'password': 'abc123'} #元数据实例，建议使用tokudb，压缩率高
PARALLEL = 5
LOGGING_LEVEL = 'info'

ARCHIVE_USER = 'dba_archive_user'  #归档账号，需要在每个归档实例都创建
ARCHIVE_PASSWORD = 'abc123'

#发送执行通知（xxx替换成企业微信WEBHOOK）
WXWORK_WEBHOOK = 'https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=xxx'
