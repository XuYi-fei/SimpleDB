# SimpleDB

SimpleDB 是一个 Golang 实现的简单的数据库，部分原理参照自 MySQL、PostgreSQL 和 SQLite，同时参考了[MYDB](https://github.com/CN-GuoZiyang/MYDB)
实现了以下功能：

- 数据的可靠性和数据恢复
- 两段锁协议（2PL）实现可串行化调度
- MVCC
- 两种事务隔离级别（读提交和可重复读）
- 死锁处理
- 简单的表和字段管理
- 简陋的 SQL 解析
- 基于 socket 的 server 和 client

## 运行方式

项目的goland版本为1.20，可以采用两种运行方式：
- 先编译服务端和客户端代码再运行
- 直接运行代码

### 1. 先编译再运行
按照如下步骤进行编译
- 编译服务端代码
```shell
go build -o db_server backend/Launcher.go
```
- 编译客户端代码
```shell
go build -o db_client client/main/Launcher.go
```
接着执行以下命令以 data/dev/dev 作为路径创建数据库：

```shell
./db_server -create data/dev/dev
```

随后通过以下命令以默认参数启动数据库服务：

```shell
./db_server -open data/dev/dev
```

这时数据库服务就已经启动在本机的 9998 端口。重新启动一个终端，执行以下命令启动客户端连接数据库：
```shell
./db_client
```

会启动一个交互式命令行，就可以在这里输入类 SQL 语法，回车会发送语句到服务，并输出执行的结果。

**一个执行示例：**

![](https://xuyifei-oss.oss-cn-beijing.aliyuncs.com/SimpleDB/SimpleDBDemo.png)

### 2. 直接使用go运行代码文件

以 data/dev/dev 作为路径创建数据库：
```shell
go run backend/Launcher.go -create data/dev/dev
```

以默认参数启动数据库服务：
```shell
go run backend/Launcher.go -open data/dev/dev
```

这时数据库服务就已经启动在本机的 9998 端口。重新启动一个终端，执行以下命令启动客户端连接数据库：
```shell
go run client/main/Launcher.go
```
