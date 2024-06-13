# spark引擎
## 一、spark-sql操作
### 1、常规操作
#### 1）创建表
```sql
-- PARTITIONED BY (partition-expressions) ：配置分区
-- LOCATION '(fully-qualified-uri)' ：指定表路径
-- COMMENT 'table documentation' ：配置表备注
-- TBLPROPERTIES ('key'='value', ...) ：配置表属性
-- 对Iceberg表的每次更改都会生成一个新的元数据文件（json文件）以提供原子性。默认情况下，旧元数据文件作为历史文件保存不会删除。
-- 如果要自动清除元数据文件，在表属性中设置write.metadata.delete-after-commit.enabled=true。这将保留一些元数据文件（直到write.metadata.previous-versions-max），并在每个新创建的元数据文件之后删除旧的元数据文件。
use hadoop_prod;
create database default;
use default;
--基础表
CREATE TABLE hadoop_prod.default.sample1 (
    id bigint COMMENT 'unique id',
    data string)
USING iceberg;
--分区表
CREATE TABLE hadoop_prod.default.sample2 (
    id bigint,
    data string,
    category string)
USING iceberg
PARTITIONED BY (category);
--隐藏分区表
CREATE TABLE hadoop_prod.default.sample3 (
    id bigint,
    data string,
    category string,
    ts timestamp)
USING iceberg
PARTITIONED BY (bucket(16, id), days(ts), category);
-- 支持的转换有:
-- years(ts):按年划分
-- months(ts):按月划分
-- days(ts)或date(ts):等效于dateint分区
-- hours(ts)或date_hour(ts):等效于dateint和hour分区
-- bucket(N, col):按哈希值划分mod N个桶
-- truncate(L, col):按截断为L的值划分
-- 字符串被截断为给定的长度
-- 整型和长型截断为bin: truncate(10, i)生成分区0,10,20,30，…
```
#### 2）删除表
```sql
-- 对于HadoopCatalog而言，运行DROP TABLE将从catalog中删除表并删除表内容。
CREATE EXTERNAL TABLE hadoop_prod.default.sample7 (
    id bigint COMMENT 'unique id',
    data string)
USING iceberg;
INSERT INTO hadoop_prod.default.sample7 values(1,'a');
DROP TABLE hadoop_prod.default.sample7;

--对于HiveCatalog而言：
-- 在0.14之前，运行DROP TABLE将从catalog中删除表并删除表内容。
-- 从0.14开始，DROP TABLE只会从catalog中删除表，不会删除数据。为了删除表内容，应该使用DROP table PURGE。
CREATE EXTERNAL TABLE hive_prod.default.sample7 (
    id bigint COMMENT 'unique id',
    data string)
USING iceberg;
INSERT INTO hive_prod.default.sample7 values(1,'a');
-- 删除表
DROP TABLE hive_prod.default.sample7;
-- 删除表和数据(谨慎使用，有问题)
DROP TABLE hive_prod.default.sample7 PURGE;
```
#### 3）修改表
Iceberg在Spark 3中完全支持ALTER TABLE，包括:

Ø 重命名表

Ø 设置或删除表属性

Ø 添加、删除和重命名列

Ø 添加、删除和重命名嵌套字段

Ø 重新排序顶级列和嵌套结构字段

Ø 扩大int、float和decimal字段的类型

Ø 将必选列变为可选列

此外，还可以使用SQL扩展来添加对分区演变的支持和设置表的写顺序。

```sql
-- 测试表
CREATE TABLE hive_prod.default.sample1 (
    id bigint COMMENT 'unique id',
    data string)
USING iceberg;
-- 查看表结构
describe formatted hive_prod.default.sample1;
```
##### Ⅰ、修改表名
```sql
ALTER TABLE hive_prod.default.sample1 RENAME TO hive_prod.default.sample2;
```
##### Ⅱ、修改表属性
```sql
-- （1）修改表属性
ALTER TABLE hive_prod.default.sample1 SET TBLPROPERTIES (
    'read.split.target-size'='268435456'
);

ALTER TABLE hive_prod.default.sample1 SET TBLPROPERTIES (
    'comment' = 'A table comment.'
);
-- （2）删除表属性
ALTER TABLE hive_prod.default.sample1 UNSET TBLPROPERTIES ('read.split.target-size');
```
##### Ⅲ 、增加列
```sql
ALTER TABLE hive_prod.default.sample1
ADD COLUMNS (
    category string comment 'new_column'
  );

-- 添加struct类型的列
ALTER TABLE hive_prod.default.sample1
ADD COLUMN point struct<x: double, y: double>;

-- 往struct类型的列中添加字段
ALTER TABLE hive_prod.default.sample1
ADD COLUMN point.z double;

-- 创建struct的嵌套数组列
ALTER TABLE hive_prod.default.sample1
ADD COLUMN points array<struct<x: double, y: double>>;

-- 在数组中的结构中添加一个字段。使用关键字'element'访问数组的元素列。
ALTER TABLE hive_prod.default.sample1
ADD COLUMN points.element.z double;

-- 创建一个包含Map类型的列，key和value都为struct类型
ALTER TABLE hive_prod.default.sample1
ADD COLUMN pointsm map<struct<x: int>, struct<a: int>>;

-- 在Map类型的value的struct中添加一个字段。
ALTER TABLE hive_prod.default.sample1
ADD COLUMN pointsm.value.b int;
-- 在Spark 2.4.4及以后版本中，可以通过添加FIRST或AFTER子句在任何位置添加列:（只能使用在HadoopCatalog下）
ALTER TABLE hadoop_prod.default.sample1
ADD COLUMN new_column1 string AFTER data;
ALTER TABLE hadoop_prod.default.sample1
ADD COLUMN new_column2 string FIRST;
```
##### Ⅳ、修改列
```sql
--（1）修改列名
ALTER TABLE hadoop_prod.default.sample1 RENAME COLUMN data TO data1;
--（2）Alter Column修改类型（只允许安全的转换）
ALTER TABLE hadoop_prod.default.sample1
ADD COLUMNS (
    idd int
  );
ALTER TABLE hadoop_prod.default.sample1 ALTER COLUMN idd TYPE bigint;
--（3）Alter Column 修改列的注释
ALTER TABLE hadoop_prod.default.sample1 ALTER COLUMN id COMMENT 'b';
--（4）Alter Column修改列的顺序
ALTER TABLE hadoop_prod.default.sample1 ALTER COLUMN id FIRST;
ALTER TABLE hadoop_prod.default.sample1 ALTER COLUMN new_column2 AFTER new_column1;
--（5）Alter Column修改列是否允许为null
ALTER TABLE hadoop_prod.default.sample1 ALTER COLUMN id DROP NOT NULL;
-- ALTER COLUMN不用于更新struct类型。使用ADD COLUMN和DROP COLUMN添加或删除struct类型的字段。
```
##### Ⅴ、删除列
```sql
ALTER TABLE hadoop_prod.default.sample1 DROP COLUMN idd;
ALTER TABLE hive_prod.default.sample1 DROP COLUMN point.z;
```
##### Ⅵ、添加分区
```sql
ALTER TABLE hive_prod.default.sample1 ADD PARTITION FIELD category ;

ALTER TABLE hive_prod.default.sample1 ADD PARTITION FIELD bucket(16, id);
ALTER TABLE hive_prod.default.sample1 ADD PARTITION FIELD truncate(data, 4);
ALTER TABLE hive_prod.default.sample1 ADD PARTITION FIELD years(ts);

ALTER TABLE hive_prod.default.sample1 ADD PARTITION FIELD bucket(16, id) AS shard;
```
##### Ⅶ、删除分区
* 注意，尽管删除了分区，但列仍然存在于表结构中。

* 删除分区字段是元数据操作，不会改变任何现有的表数据。新数据将被写入新的分区，但现有数据将保留在旧的分区布局中。

* 当分区发生变化时，动态分区覆盖行为也会发生变化。例如，如果按天划分分区，而改为按小时划分分区，那么覆盖将覆盖每小时划分的分区，而不再覆盖按天划分的分区。

* 删除分区字段时要小心，可能导致元数据查询失败或产生不同的结果。

````sql
ALTER TABLE hadoop_prod.default.sample1 DROP PARTITION FIELD category;
ALTER TABLE hadoop_prod.default.sample1 DROP PARTITION FIELD bucket(16, id);
ALTER TABLE hadoop_prod.default.sample1 DROP PARTITION FIELD truncate(data, 4);
ALTER TABLE hadoop_prod.default.sample1 DROP PARTITION FIELD years(ts);
ALTER TABLE hadoop_prod.default.sample1 DROP PARTITION FIELD shard;
````
##### Ⅷ、修改分区
```sql
ALTER TABLE hive_prod.default.sample1 REPLACE PARTITION FIELD bucket(16, id) WITH bucket(8, id);
```
##### Ⅸ、修改表写入顺序
```sql
ALTER TABLE hive_prod.default.sample1 WRITE ORDERED BY category, id;

ALTER TABLE hive_prod.default.sample1 WRITE ORDERED BY category ASC, id DESC;

ALTER TABLE hive_prod.default.sample1 WRITE ORDERED BY category ASC NULLS LAST, id DESC NULLS FIRST;
-- 表写顺序不能保证查询的数据顺序。它只影响数据写入表的方式。
-- WRITE ORDERED BY设置了一个全局排序，即跨任务的行排序，就像在INSERT命令中使用ORDER BY一样:
INSERT INTO hive_prod.default.sample1
SELECT id, data, category, ts FROM another_table
ORDER BY ts, category;
-- 要在每个任务内排序，而不是跨任务排序，使用local ORDERED BY:
ALTER TABLE hive_prod.default.sample1 WRITE LOCALLY ORDERED BY category, id;
```
##### Ⅹ、按分区并行写入
```sql
ALTER TABLE hive_prod.default.sample1 WRITE DISTRIBUTED BY PARTITION LOCALLY ORDERED BY category, id;
```
#### 4）插入数据
```sql
CREATE TABLE hadoop_prod.default.a (
    id bigint,
    count bigint)
USING iceberg;

CREATE TABLE hadoop_prod.default.b (
    id bigint,
count bigint,
flag string)
USING iceberg;

-- 1）Insert Into
INSERT INTO hadoop_prod.default.a VALUES (1, 1), (2, 2), (3, 3);
INSERT INTO hadoop_prod.default.b VALUES (1, 1, 'a'), (2, 2, 'b'), (4, 4, 'd');
-- 2）MERGE INTO行级更新
MERGE INTO hadoop_prod.default.a t 
USING (SELECT * FROM hadoop_prod.default.b) u ON t.id = u.id
WHEN MATCHED AND u.flag='b' THEN UPDATE SET t.count = t.count + u.count
WHEN MATCHED AND u.flag='a' THEN DELETE
WHEN NOT MATCHED THEN INSERT (id,count) values (u.id,u.count);
```
#### 5）查询数据
```sql
-- 0）创建表并插入数据
CREATE EXTERNAL TABLE hadoop_prod.default.sample8 (
    id bigint COMMENT 'unique id',
    data string)
USING iceberg;
INSERT INTO hadoop_prod.default.sample8 values(1,'a');
INSERT INTO hadoop_prod.default.sample8 values(2,'c');
INSERT INTO hadoop_prod.default.sample8 values(3,'b');

-- 1）普通查询
SELECT count(1) as count, data
FROM hadoop_prod.default.sample8
GROUP BY data;
-- 2）查询元数据
-- // 查询表快照
SELECT * FROM hadoop_prod.default.sample8.snapshots;

-- // 查询数据文件信息
SELECT * FROM hadoop_prod.default.sample8.files;

-- // 查询表历史
SELECT * FROM hadoop_prod.default.sample8.history;

-- // 查询 manifest
SELECT * FROM hadoop_prod.default.sample8.manifests;
```
#### 6）存储过程
Procedures可以通过CALL从任何已配置的Iceberg Catalog中使用。所有Procedures都在namespace中。
##### Ⅰ、语法
```sql
-- 按照参数名传参
CALL catalog_name.system.procedure_name(arg_name_2 => arg_2, arg_name_1 => arg_1)
-- 当按位置传递参数时，如果结束参数是可选的，则只有结束参数可以省略。
CALL catalog_name.system.procedure_name(arg_1, arg_2, ... arg_n)
```
##### Ⅱ、快照管理
```sql
-- // 查询表快照
SELECT * FROM hadoop_prod.default.sample8.snapshots;
-- （1）回滚到指定的快照id
CALL hadoop_prod.system.rollback_to_snapshot('default.sample8', 3394509036086431205);
-- （2）回滚到指定时间的快照
CALL hadoop_prod.system.rollback_to_timestamp('default.sample8', TIMESTAMP '2023-05-29 19:58:00.008');
-- （3）设置表的当前快照ID
CALL hadoop_prod.system.set_current_snapshot('default.sample8', 6476372975407164130);
-- （4）从快照变为当前表状态
CALL hadoop_prod.system.cherrypick_snapshot('default.sample8', 6476372975407164130);
-- 这个有问题，不要用这个写法切换快照
CALL hadoop_prod.system.cherrypick_snapshot(snapshot_id => 3394509036086431205, table => 'default.sample8' );
```
##### Ⅲ、元数据管理
````sql
-- // 查询表快照
SELECT * FROM hadoop_prod.default.sample8.snapshots;

--（1）删除早于指定日期和时间的快照，但保留最近100个快照:
CALL hadoop_prod.system.expire_snapshots('default.sample8', TIMESTAMP '2023-05-29 19:57:59.598', 100);
--（2）删除Iceberg表中任何元数据文件中没有引用的文件
--#列出所有需要删除的候选文件
CALL hadoop_prod.system.remove_orphan_files(table => 'default.sample8', dry_run => true);

--#删除指定目录中db.sample表不知道的任何文件
CALL hadoop_prod.system.remove_orphan_files(table => 'default.sample8', location => 'tablelocation/data');
--（3）合并数据文件（合并小文件）
CALL hadoop_prod.system.rewrite_data_files('default.sample8');

CALL hadoop_prod.system.rewrite_data_files(table => 'default.sample8', strategy => 'sort', sort_order => 'id DESC NULLS LAST,name ASC NULLS FIRST');

CALL hadoop_prod.system.rewrite_data_files(table => 'default.sample8', strategy => 'sort', sort_order => 'zorder(c1,c2)');

CALL hadoop_prod.system.rewrite_data_files(table => 'default.sample8', options => map('min-input-files','2'));

CALL hadoop_prod.system.rewrite_data_files(table => 'default.sample8', where => 'id = 3 and name = "foo"');
--（4）重写表清单来优化执行计划
CALL hadoop_prod.system.rewrite_manifests('default.sample8');

--#重写表db中的清单。并禁用Spark缓存的使用。这样做可以避免执行程序上的内存问题。
CALL hadoop_prod.system.rewrite_manifests('db.sample', false);
````
##### Ⅳ、迁移表
```sql
-- // 查询表快照
SELECT * FROM hadoop_prod.default.sample8.snapshots;

--（1）快照
CALL hadoop_prod.system.snapshot('default.sample8','default.sample8_1');
CALL hadoop_prod.system.snapshot('default.sample8', 'default.sample9', '/tmp/temptable/');
--（2）迁移
CALL hadoop_prod.system.migrate('hive_prod.default.sample8', map('foo', 'bar'))
CALL hadoop_prod.system.migrate('default.sample8')
--（3）添加数据文件
CALL hive_prod.system.add_files(
table => 'default.sample9',
source_table => 'default.sample8'
);

CALL hadoop_prod.system.add_files(
  table => 'db.tbl',
  source_table => '`parquet`.`path/to/table`'
)
```
##### Ⅴ、元数据信息
```sql
--（1）获取指定快照的父快照id
CALL hadoop_prodhadoop_prodhadoop_prod.system.ancestors_of('default.sample8')
--（2）获取指定快照的所有祖先快照
CALL hadoop_prod.system.ancestors_of('default.sample8', 3394509036086431205);
CALL hadoop_prod.system.ancestors_of(snapshot_id => 3394509036086431205, table => 'default.sample8')
```
# flink引擎
## 一、flinkSql常规操作
### 1、catalog操作
```sql
-- hive catalog
CREATE CATALOG hadoop_catalog1 WITH (
  'type'='iceberg',
  'catalog-type'='hadoop',
  'warehouse'='hdfs://node49:9000/iceberg',
  'property-version'='1'
);

-- hadoop catalog
CREATE CATALOG hive_catalog1 WITH (
  'type'='iceberg',
  'catalog-type'='hive',
  'uri'='thrift://node86:9083',
  'clients'='5',
  'property-version'='1',
  'warehouse'='hdfs://node49:9000/user/hive/warehouse'
);

-- 查看catalog
show catalogs;

-- 使用catalog
use catalog hive_catalog;
```
### 2、DDL语句
#### 1）创建数据库
```sql
-- 创建库
create database flinkdb;
-- 查询库
show databases;
-- 切换库
use flinkdb;
```
#### 2）创建表
```sql
-- 基础表
CREATE TABLE `hadoop_catalog`.`flinkdb`.`sample` (
    id BIGINT COMMENT 'unique id',
    data STRING
);
-- 分区表
CREATE TABLE `hive_catalog`.`flinkdb`.`sample` (
    id BIGINT COMMENT 'unique id',
    data STRING
) PARTITIONED BY (data);
-- 复制结构建表
CREATE TABLE  `hive_catalog`.`flinkdb`.`sample_like` LIKE `hive_catalog`.`flinkdb`.`sample`;
--建表指定catalog
CREATE TABLE flink_table (
    id   BIGINT,
    data STRING
) WITH (
    'connector'='iceberg',
    'catalog-name'='hive_prod',
    'uri'='thrift://node86:9083',
    'warehouse'='hdfs://node49:9000/user/hive/warehouse'
);

--支持upsert命令的表结构
CREATE TABLE `hive_catalog`.`flinkdb`.`flink2` (
  `id`  INT UNIQUE COMMENT 'unique id',
  `data` STRING NOT NULL,
 PRIMARY KEY(`id`) NOT ENFORCED
) with ('format-version'='2', 'write.upsert.enabled'='true');
```
#### 3）修改表
```sql
-- 1）修改表属性
ALTER TABLE `hive_catalog`.`flinkdb`.`sample` SET ('write.format.default'='avro');
-- 2）修改表名 hadoop catalog不支持修改表名
ALTER TABLE `hive_catalog`.`flinkdb`.`sample` RENAME TO `hive_catalog`.`default`.`new_sample`;
```
#### 4）删除表
```sql
DROP TABLE `hive_catalog`.`flinkdb`.`new_sample`;
```
### 3、DML语句
#### 1）插入语句
```sql
-- 基础插入
INSERT INTO `hive_catalog`.`flinkdb`.`sample` VALUES (1, 'a');
-- 表查询结果插入
INSERT INTO `hive_catalog`.`flinkdb`.`sample_like` SELECT id, data from `hive_catalog`.`flinkdb`.`sample`;
-- 基础批量插入数据
INSERT INTO flink_table VALUES (1, 'AAA'), (2, 'BBB'), (3, 'CCC');

--不可用：报错无界数据流不支持overwrite
INSERT OVERWRITE flink_table VALUES (5, 'a');

-- upsert(没看出upsert)
INSERT INTO flink2 /*+ OPTIONS('upsert-enabled'='true') */ values(1,'e');
```
#### 2）查询语句
```sql
-- 表结构返回查询结果
SET execution.result-mode=tableau;
SELECT * FROM hive_catalog.flinkdb.flink2;


-- 批量读
SET execution.runtime-mode = batch;
SELECT * FROM hive_catalog.flinkdb.flink2;

-- 流式读：任务在yarn-session客户端上不会中止
SET execution.runtime-mode = streaming;
SET table.dynamic-table-options.enabled=true;
SELECT * FROM hive_catalog.flinkdb.flink2 /* +options('streaming'='true','minitor-interval'='1s') */;
```

