# redis_info_collect
基于python3.5环境
依懒包：pip install redis elasticsearch

文件存放目录：
/opt/slowlog/
host.txt #一行一台redis实例服务器IP地址

准备环境：
1、redis端配置：
slowlog-log-slower-than 10000         
#执行命令超过100ms就被记录为慢查询
slowlog-max-len 1024                  
#保留最近1024条慢查询记录

2、elasticsearch集群
添加redis_slowlog索引
