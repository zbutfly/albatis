# 基于Parquet文件的Hive数据输出模块设计

> Design of hive output based on rolling/hashed parquet File

## 需求定义

该模块属于albatis框架，负责支持基于`parquet数据格式的hdfs文件`的Hive表。在当前业务场景中，应满足以下需求：

1. Hive表根据时间分区。
2. 每个parquet文件写入大小有上限。
4. 写入数据在Hive中生效有时限。

## 应用接口（API）设计

### URI Schema

HiveParquetOutput应支持如下URI Schema：

hive:hdfs:`parquet`//`hdfs配置描述文件位置或服务地址描述`/-/`基准目录`/`目录分表策略描述`

- hdfs配置描述文件位置或服务地址描述：包含hdfs配置文件本地路径。
  - 为空：当前classpath下。
  - 不为空：目录/zip包本地或远程路径。
  
- 文件系统基准目录（数据根目录）：分表hash子目录开始路径，为一个HDFS文件系统上的存在的路径。

### 按表粒度的输出参数配置

通过RESTful接口获取

- 接口返回配置描述

~~~json
{
    '表分区字段': "BIRTHDAY",
    '表分区策略':"table_name/year=yyyy-MM-dd/month=mm/day=dd“,
    '文件滚动大小阈值':268435456,		// 256M
    '文件滚动记录条数阈值':5000,	// 条数
    '文件刷新最大间隔':3600		// 1小时
}
~~~

- 接口调用伪代码

~~~java
Map<String, Object> params = REST.get("table_name", "http://.../...");
for (record: records) {
	SimpleDateFormat.format(record.get("BIRTHDAY"), params.get("表分区策略"));
}
//实际写入目录：table_name/year=2019/month=11/day=1
~~~

### 参数配置

参数配置以key-value对形式的properties配置。

- 表配置参数获取uri：用于RESTful获取表参数。
- Hive集群jdbc uri：用于执行hive SQL，如refresh。


### DDL

开始数据输出前应确认hive分表建立（外部表映射）完成。使用Hive规范的SQL语句通过JDBC实现。该部分实现应集成在当前DDL平台。

## 底层操作实现

### 目录分表

1. Hive表根据时间分表。分表策略可以按天或按小时，根据`参数配置`定义解析和实现可扩展性。
2. 每张分表映射HDFS文件系统的一个子目录中的所有.parquet文件。

### 文件数据写入

1. 数据输入流中的每条源数据记录应确定自身所属分表目录。
2. 由HiveParquetOutput序列化记录数据，写入该目录下的current.parquet.tmp文件。
3. 每条记录的写入操作在目录上加锁，成为原子操作，顺序执行。
4. 属于多个分表目录的数据记录写入可以并发执行。
5. 写入原子操作结束后判断是否需要rolling/refreshing。

### 文件滚动

1. 每个parquet文件写入最大byte不超过`参数配置`定义的文件大小阈值。
2. 每个parquet文件写入最大记录条数不超过`参数配置`定义的文件记录阈值。
3. 每个parquet文件写入最大时间不超过`参数配置`定义的文件时间阈值。
4. 达到该阈值，Rolling新文件：关闭的写入完成文件，改名，refresh到hive数据表中，并增量refresh当前新文件（待确认是否支持）。

## Parquet数据格式

### 数据序列化

### 数据反序列化
