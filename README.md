## vcomcdc

 使用flinkcdc stream api进行数据库同步的工具：

目前实现了mariadb到mariadb的测试。



# 使用方式：



安装Flink

目前在flink-1.17.1进行了测试。

**flinkcdclib**

主要slot的配置

taskmanager.numberOfTaskSlots: 8

rest.address: 0.0.0.0

rest.bind-address: 0.0.0.0

目前用到【flinkcdclib文件中】复制同步依赖的flin到lib/中。



**启动flink的jobmanagar，taskmanger**

/flink/bin/start-cluster.sh

关闭：

/flink/bin/stop-cluster.sh





## 运行程序：

$ /home/admin/flink/bin/flink run -d -c com.zzvcom.cdc.mysql.MySqlDataStreamJob /home/admin/cdcconfig/vcom-client-cdc-1.0.0.jar /home/admin/cdcconfig/TestMysqlToJdbc.properties



本例子中flink安装在/home/admin/下面：

编译好的jar为vcom-client-cdc-1.0.0.jar 

程序jar放置的位置为：/home/admin/cdcconfig/vcom-client-cdc-1.0.0.jar

命令行参数为配置文件的路径：/home/admin/cdcconfig/TestMysqlToJdbc.properties





TestMysqlToJdbc.properties的配置信息如下：

```
#源数据库mysql
#MySQL 数据库服务器的 IP 地址或主机名。43 44
#hostname=192.168.175.12
hostname=192.168.175.44
#MySQL 数据库服务器的整数端口号。
#port=15002
port=3306
#连接到 MySQL 数据库服务器时要使用的 MySQL 用户的名称。
username=rmscdc
#连接 MySQL 数据库服务器时使用的密码。
password=rmscdc@A123456
#要监视的 MySQL 服务器的数据库名称。数据库名称还支持正则表达式，以监视多个与正则表达式匹配的表。
database-name=nrms
#需要监视的 MySQL 数据库的表名。表名支持正则表达式，以监视满足正则表达式的多个表。注意：MySQL CDC 连接器在正则匹配表名时，会把用户填写的 database-name， table-name 通过字符串 `\\.` 连接成一个全路径的正则表达式，然后使用该正则表达式和 MySQL 数据库中表的全限定名进行正则匹配
table-name=nrms.*
server-id=5400
#读取数据使用的 server id，server id 可以是个整数或者一个整数范围，比如 '5400' 或 '5400-5408', 建议在 'scan.incremental.snapshot.enabled' 参数为启用时，配置成整数范围。因为在当前 MySQL 集群中运行的所有 slave 节点，标记每个 salve 节点的 id 都必须是唯一的。 所以当连接器加入 MySQL 集群作为另一个 slave 节点（并且具有唯一 id 的情况下），它就可以读取 binlog。 默认情况下，连接器会在 5400 和 6400 之间生成一个随机数，但是我们建议用户明确指定 Server id。
scan.incremental.snapshot.enabled=true
#表快照的块大小（行数），读取表的快照时，捕获的表被拆分为多个块。默认8096
scan.incremental.snapshot.chunk.size=8096
#读取表快照时每次读取数据的最大条数。默认1024
scan.snapshot.fetch.size=1024
#MySQL CDC 消费者可选的启动模式， 合法的模式为 "initial"，"earliest-offset"，"latest-offset"，"specific-offset" 和 "timestamp"。 请查阅 启动模式 章节了解更多详细信息。
#-------------启动模式描述------------
#initial （默认）：在第一次启动时对受监视的数据库表执行初始快照，并继续读取最新的 binlog。
#earliest-offset：跳过快照阶段，从可读取的最早 binlog 位点开始读取
#latest-offset：首次启动时，从不对受监视的数据库表执行快照， 连接器仅从 binlog 的结尾处开始读取，这意味着连接器只能读取在连接器启动之后的数据更改。
#specific-offset：跳过快照阶段，从指定的 binlog 位点开始读取。位点可通过 binlog 文件名和位置指定，或者在 GTID 在集群上启用时通过 GTID 集合指定。
#timestamp：跳过快照阶段，从指定的时间戳开始读取 binlog 事件。
scan.startup.mode=initial
#yyyy-MM-dd HH:mm:ss 或者时间戳#'1667232000000' -- 在时间戳启动模式下指定启动时间戳
scan.startup.timestamp=
#'mysql-bin.000003', -- 在特定位点启动模式下指定 binlog 文件名
scan.startup.specific-offset.file=
#'4', -- 在特定位点启动模式下指定 binlog 位置
scan.startup.specific-offset.pos=
#'24DA167-0C0C-11E8-8442-00059A3C7B00:1-19', -- 在特定位点启动模式下指定 GTID 集合
scan.startup.specific-offset.gtid-set=
#--------------------------------
#目标数据库
dest.jdbc.url=jdbc:mysql://172.18.252.140:2883/?useUnicode=true&useSSL=false&serverTimezone=Asia/Shanghai&characterEncoding=utf-8&allowMultiQueries=true&rewriteBatchedStatements=true&connectTimeout=300000&autoReconnect=true
#dest.jdbc.url=jdbc:mysql://172.18.252.140:2881/?useUnicode=true&useSSL=false&serverTimezone=Asia/Shanghai&characterEncoding=utf-8&connectTimeout=300000&autoReconnect=true&cacheServerConfiguration=true&useServerPrepStmts=true&cachePrepStmts=true&rewriteBatchedStatements=true&useBatchMultiSend=true&useBatchMultiSendNumber=1000
dest.jdbc.username=root@mq_t1
dest.jdbc.password=vcom123456
dest.jdbc.driver=org.mariadb.jdbc.Driver
#执行异常的最大重试次数
dest.jdbc.batch.maxTryNum=3
#每批插入最大大小
dest.jdbc.batch.batchSize=1024
#批量定时的频率
dest.jdbc.batch.batchIntervalMs=2000

# 1、直接模式 2生产者消费者模式

dest.jdbc.batch.model=3
#2生产消费者模式的时候,缓存的容量多少批。
dest.jdbc.batch.model.batchBuffSize=4

```







## **编程问题记录：**

比如我做的是mariadb、oceanbase组合的stream api同步。

我就把：

flink-sql-connector-mysql-cdc-2.4.1.jar

flink-sql-connector-oceanbase-cdc-2.4.1.jar

复制同步依赖的flinkcdc的jar到lib/中

之后如果对应的jar存在相关的类，就采用jar里面的东西。

比如：

import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Field;

import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Schema;

import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Struct;

import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.source.SourceRecord;

另外是mysql驱动的问题：

应用上述的flink-sql-connector-*.jar都存在mysql驱动，当时版本有冲突，那么，我向flink/lib中放入一个

mariadb-java-client-2.7.9.jar ，之后使用org.mariadb.jdbc.Driver 作为驱动名字，验证没有问题。

希望有所帮助。
