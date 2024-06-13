<template>
  <div class="icebergArea">
    <h1>spark引擎</h1>
    <h2>一、spark-sql操作</h2>
    <h3> 1、常规操作</h3>
    <h4> 1）创建表</h4>
    <div class="icebergSqlArea">
        <p>-- PARTITIONED BY (partition-expressions) ：配置分区</p>
        <p>-- LOCATION '(fully-qualified-uri)' ：指定表路径</p>
        <p>-- COMMENT 'table documentation' ：配置表备注</p>
        <p>-- TBLPROPERTIES ('key'='value', ...) ：配置表属性</p>
        <p>-- 对Iceberg表的每次更改都会生成一个新的元数据文件（json文件）以提供原子性。默认情况下，旧元数据文件作为历史文件保存不会删除。</p>
        <p>-- 如果要自动清除元数据文件，在表属性中设置write.metadata.delete-after-commit.enabled=true。这将保留一些元数据文件（直到write.metadata.previous-versions-max），并在每个新创建的元数据文件之后删除旧的元数据文件。</p>
        <p>use hadoop_prod;</p>
        <p>create database default;</p>
        <p>use default;</p>
        <p>--基础表</p>
        <p>CREATE TABLE hadoop_prod.default.sample1 (</p>
        <p>    id bigint COMMENT 'unique id',</p>
        <p>    data string)</p>
        <p>USING iceberg;</p>
        <p>--分区表</p>
        <p>CREATE TABLE hadoop_prod.default.sample2 (</p>
        <p>    id bigint,</p>
        <p>    data string,</p>
        <p>    category string)</p>
        <p>USING iceberg</p>
        <p>PARTITIONED BY (category);</p>
        <p>--隐藏分区表</p>
        <p>CREATE TABLE hadoop_prod.default.sample3 (</p>
        <p>    id bigint,</p>
        <p>    data string,</p>
        <p>    category string,</p>
        <p>    ts timestamp)</p>
        <p>USING iceberg</p>
        <p>PARTITIONED BY (bucket(16, id), days(ts), category);</p>
        <p>-- 支持的转换有:</p>
        <p>-- years(ts):按年划分</p>
        <p>-- months(ts):按月划分</p>
        <p>-- days(ts)或date(ts):等效于dateint分区</p>
        <p>-- hours(ts)或date_hour(ts):等效于dateint和hour分区</p>
        <p>-- bucket(N, col):按哈希值划分mod N个桶</p>
        <p>-- truncate(L, col):按截断为L的值划分</p>
        <p>-- 字符串被截断为给定的长度</p>
        <p>-- 整型和长型截断为bin: truncate(10, i)生成分区0,10,20,30，… </p>
    </div>

    <h4>2）删除表</h4>
    <div class="icebergSqlArea">
        <p>-- 对于HadoopCatalog而言，运行DROP TABLE将从catalog中删除表并删除表内容。</p>
        <p>CREATE EXTERNAL TABLE hadoop_prod.default.sample7 (</p>
        <p>    id bigint COMMENT 'unique id',</p>
        <p>    data string)</p>
        <p>USING iceberg;</p>
        <p>INSERT INTO hadoop_prod.default.sample7 values(1,'a');</p>
        <p>DROP TABLE hadoop_prod.default.sample7;</p>
        <p>--对于HiveCatalog而言：</p>
        <p>-- 在0.14之前，运行DROP TABLE将从catalog中删除表并删除表内容。</p>
        <p>-- 从0.14开始，DROP TABLE只会从catalog中删除表，不会删除数据。为了删除表内容，应该使用DROP table PURGE。</p>
        <p>CREATE EXTERNAL TABLE hive_prod.default.sample7 (</p>
        <p>    id bigint COMMENT 'unique id',</p>
        <p>    data string)</p>
        <p>USING iceberg;</p>
        <p>INSERT INTO hive_prod.default.sample7 values(1,'a');</p>
        <p>-- 删除表</p>
        <p>DROP TABLE hive_prod.default.sample7;</p>
        <p>-- 删除表和数据(谨慎使用，有问题)</p>
        <p>DROP TABLE hive_prod.default.sample7 PURGE;</p>
    </div>
    <h4> 3）修改表</h4>
    <p>Iceberg在Spark 3中完全支持ALTER TABLE，包括:</p>

    <p>Ø 重命名表</p>

    <p>Ø 设置或删除表属性</p>

    <p>Ø 添加、删除和重命名列</p>

    <p>Ø 添加、删除和重命名嵌套字段</p>

    <p>Ø 重新排序顶级列和嵌套结构字段</p>

    <p>Ø 扩大int、float和decimal字段的类型</p>

    <p>Ø 将必选列变为可选列</p>

    <p>此外，还可以使用SQL扩展来添加对分区演变的支持和设置表的写顺序。</p>
    <div class="icebergSqlArea">
        <p>-- 测试表</p>
        <p>CREATE TABLE hive_prod.default.sample1 (</p>
        <p>    id bigint COMMENT 'unique id',</p>
        <p>    data string)</p>
        <p>USING iceberg;</p>
        <p>-- 查看表结构</p>
        <p>describe formatted hive_prod.default.sample1;</p>
    </div>

    <h5> Ⅰ、修改表名</h5>
    <div>
        ALTER TABLE hive_prod.default.sample1 RENAME TO hive_prod.default.sample2;
    </div>
    <h5> Ⅱ、修改表属性</h5>
    <div>
        <p>-- （1）修改表属性</p>
        <p>ALTER TABLE hive_prod.default.sample1 SET TBLPROPERTIES (</p>
        <p>    'read.split.target-size'='268435456'</p>
        <p>);</p>
        <p>ALTER TABLE hive_prod.default.sample1 SET TBLPROPERTIES (</p>
        <p>    'comment' = 'A table comment.'</p>
        <p>);</p>
        <p>-- （2）删除表属性</p>
        <p>ALTER TABLE hive_prod.default.sample1 UNSET TBLPROPERTIES ('read.split.target-size');</p>
    </div>
    <h5> Ⅲ 、增加列</h5>
    <div class="icebergSqlArea">
    <p>ALTER TABLE hive_prod.default.sample1</p>
    <p>ADD COLUMNS (</p>
    <p>    category string comment 'new_column'</p>
    <p>);</p>

    <p>-- 添加struct类型的列</p>
    <p>ALTER TABLE hive_prod.default.sample1</p>
    <p v-html="'ADD COLUMN point struct<x: double, y: double>;'"></p>

    <p>-- 往struct类型的列中添加字段</p>
    <p>ALTER TABLE hive_prod.default.sample1</p>
    <p>ADD COLUMN point.z double;</p>

    <p>-- 创建struct的嵌套数组列</p>
    <p>ALTER TABLE hive_prod.default.sample1</p>
    <p v-html="'ADD COLUMN points array <struct<x: double, y: double>>;'"></p>

    <p>-- 在数组中的结构中添加一个字段。使用关键字'element'访问数组的元素列。</p>
    <p>ALTER TABLE hive_prod.default.sample1</p>
    <p>ADD COLUMN points.element.z double;</p>

    <p>-- 创建一个包含Map类型的列，key和value都为struct类型</p>
    <p>ALTER TABLE hive_prod.default.sample1</p>
    <p v-html="'ADD COLUMN pointsm map <struct<x: int>, struct<a: int>>;'"></p>

    <p>-- 在Map类型的value的struct中添加一个字段。</p>
    <p>ALTER TABLE hive_prod.default.sample1</p>
    <p>ADD COLUMN pointsm.value.b int;</p>
    <p>-- 在Spark 2.4.4及以后版本中，可以通过添加FIRST或AFTER子句在任何位置添加列:（只能使用在HadoopCatalog下）</p>
    <p>ALTER TABLE hadoop_prod.default.sample1</p>
    <p>ADD COLUMN new_column1 string AFTER data;</p>
    <p>ALTER TABLE hadoop_prod.default.sample1</p>
    <p>ADD COLUMN new_column2 string FIRST;</p>
    </div>
    <h5> Ⅳ、修改列</h5>
    <div class="icebergSqlArea">
        <p>--（1）修改列名</p>
        <p>ALTER TABLE hadoop_prod.default.sample1 RENAME COLUMN data TO data1;</p>
        <p>--（2）Alter Column修改类型（只允许安全的转换）</p>
        <p>ALTER TABLE hadoop_prod.default.sample1</p>
        <p>ADD COLUMNS (</p>
        <p>    idd int</p>
        <p>);</p>
        <p>ALTER TABLE hadoop_prod.default.sample1 ALTER COLUMN idd TYPE bigint;</p>
        <p>--（3）Alter Column 修改列的注释</p>
        <p>ALTER TABLE hadoop_prod.default.sample1 ALTER COLUMN id COMMENT 'b';</p>
        <p>--（4）Alter Column修改列的顺序</p>
        <p>ALTER TABLE hadoop_prod.default.sample1 ALTER COLUMN id FIRST;</p>
        <p>ALTER TABLE hadoop_prod.default.sample1 ALTER COLUMN new_column2 AFTER new_column1;</p>
        <p>--（5）Alter Column修改列是否允许为null</p>
        <p>ALTER TABLE hadoop_prod.default.sample1 ALTER COLUMN id DROP NOT NULL;</p>
        <p>-- ALTER COLUMN不用于更新struct类型。使用ADD COLUMN和DROP COLUMN添加或删除struct类型的字段。</p>
    </div>
    <h5> Ⅴ、删除列</h5>
    <div class="icebergSqlArea">
        <p>ALTER TABLE hadoop_prod.default.sample1 DROP COLUMN idd;</p>
        <p>ALTER TABLE hive_prod.default.sample1 DROP COLUMN point.z;</p>
    </div>
    <h5> Ⅵ、添加分区</h5>
    <div class="icebergSqlArea">
        <p>ALTER TABLE hive_prod.default.sample1 ADD PARTITION FIELD category ;</p>

        <p>ALTER TABLE hive_prod.default.sample1 ADD PARTITION FIELD bucket(16, id);</p>
        <p>ALTER TABLE hive_prod.default.sample1 ADD PARTITION FIELD truncate(data, 4);</p>
        <p>ALTER TABLE hive_prod.default.sample1 ADD PARTITION FIELD years(ts);</p>

        <p>ALTER TABLE hive_prod.default.sample1 ADD PARTITION FIELD bucket(16, id) AS shard;</p>
    </div>
    <h5>Ⅶ、删除分区</h5>
    <p>* 注意，尽管删除了分区，但列仍然存在于表结构中。</p>

    <p>* 删除分区字段是元数据操作，不会改变任何现有的表数据。新数据将被写入新的分区，但现有数据将保留在旧的分区布局中。</p>

    <p>* 当分区发生变化时，动态分区覆盖行为也会发生变化。例如，如果按天划分分区，而改为按小时划分分区，那么覆盖将覆盖每小时划分的分区，而不再覆盖按天</p>划分的分区。

    <p>* 删除分区字段时要小心，可能导致元数据查询失败或产生不同的结果。</p>

    <div class="icebergSqlArea">
        <p>ALTER TABLE hadoop_prod.default.sample1 DROP PARTITION FIELD category;</p>
        <p>ALTER TABLE hadoop_prod.default.sample1 DROP PARTITION FIELD bucket(16, id);</p>
        <p>ALTER TABLE hadoop_prod.default.sample1 DROP PARTITION FIELD truncate(data, 4);</p>
        <p>ALTER TABLE hadoop_prod.default.sample1 DROP PARTITION FIELD years(ts);</p>
        <p>ALTER TABLE hadoop_prod.default.sample1 DROP PARTITION FIELD shard;</p>
    </div>
    <h5>Ⅷ、修改分区</h5>
    <div>
        ALTER TABLE hive_prod.default.sample1 REPLACE PARTITION FIELD bucket(16, id) WITH bucket(8, id);
    </div>
    <h5>Ⅸ、修改表写入顺序</h5>
    <div>
        <p>ALTER TABLE hive_prod.default.sample1 WRITE ORDERED BY category, id;</p>

        <p>ALTER TABLE hive_prod.default.sample1 WRITE ORDERED BY category ASC, id DESC;</p>

        <p>ALTER TABLE hive_prod.default.sample1 WRITE ORDERED BY category ASC NULLS LAST, id DESC NULLS FIRST;</p>
        <p>-- 表写顺序不能保证查询的数据顺序。它只影响数据写入表的方式。</p>
        <p>-- WRITE ORDERED BY设置了一个全局排序，即跨任务的行排序，就像在INSERT命令中使用ORDER BY一样:</p>
        <p>INSERT INTO hive_prod.default.sample1</p>
        <p>SELECT id, data, category, ts FROM another_table</p>
        <p>ORDER BY ts, category;</p>
        <p>-- 要在每个任务内排序，而不是跨任务排序，使用local ORDERED BY:</p>
        <p>ALTER TABLE hive_prod.default.sample1 WRITE LOCALLY ORDERED BY category, id;</p>
    </div>
    <h5>Ⅹ、按分区并行写入</h5>
    <div>
        ALTER TABLE hive_prod.default.sample1 WRITE DISTRIBUTED BY PARTITION LOCALLY ORDERED BY category, id;
    </div>
    <h4>4）插入数据</h4>
    <div class="icebergSqlArea">
        <p>CREATE TABLE hadoop_prod.default.a (</p>
        <p>    id bigint,</p>
        <p>    count bigint)</p>
        <p>USING iceberg;</p>

        <p>CREATE TABLE hadoop_prod.default.b (</p>
        <p>    id bigint,</p>
        <p>count bigint,</p>
        <p>flag string)</p>
        <p>USING iceberg;</p>

        <p>-- 1）Insert Into</p>
        <p>INSERT INTO hadoop_prod.default.a VALUES (1, 1), (2, 2), (3, 3);</p>
        <p>INSERT INTO hadoop_prod.default.b VALUES (1, 1, 'a'), (2, 2, 'b'), (4, 4, 'd');</p>
        <p>-- 2）MERGE INTO行级更新</p>
        <p>MERGE INTO hadoop_prod.default.a t </p>
        <p>USING (SELECT * FROM hadoop_prod.default.b) u ON t.id = u.id</p>
        <p>WHEN MATCHED AND u.flag='b' THEN UPDATE SET t.count = t.count + u.count</p>
        <p>WHEN MATCHED AND u.flag='a' THEN DELETE</p>
        <p>WHEN NOT MATCHED THEN INSERT (id,count) values (u.id,u.count);</p>
    </div>
    <h4>5）查询数据</h4>
    <div class="icebergSqlArea">
        <p>-- 0）创建表并插入数据</p>
        <p>CREATE EXTERNAL TABLE hadoop_prod.default.sample8 (</p>
        <p>    id bigint COMMENT 'unique id',</p>
        <p>    data string)</p>
        <p>USING iceberg;</p>
        <p>INSERT INTO hadoop_prod.default.sample8 values(1,'a');</p>
        <p>INSERT INTO hadoop_prod.default.sample8 values(2,'c');</p>
        <p>INSERT INTO hadoop_prod.default.sample8 values(3,'b');</p>

        <p>-- 1）普通查询</p>
        <p>SELECT count(1) as count, data</p>
        <p>FROM hadoop_prod.default.sample8</p>
        <p>GROUP BY data;</p>
        <p>-- 2）查询元数据</p>
        <p>-- // 查询表快照</p>
        <p>SELECT * FROM hadoop_prod.default.sample8.snapshots;</p>

        <p>-- // 查询数据文件信息</p>
        <p>SELECT * FROM hadoop_prod.default.sample8.files;</p>

        <p>-- // 查询表历史</p>
        <p>SELECT * FROM hadoop_prod.default.sample8.history;</p>

        <p>-- // 查询 manifest</p>
        <p>SELECT * FROM hadoop_prod.default.sample8.manifests;</p>
    </div>
    <h4> 6）存储过程</h4>
    Procedures可以通过CALL从任何已配置的Iceberg Catalog中使用。所有Procedures都在namespace中。
    <h5> Ⅰ、语法</h5>
    <div class="icebergSqlArea">
        <p>-- 按照参数名传参</p>
        <p>CALL catalog_name.system.procedure_name(arg_name_2 => arg_2, arg_name_1 => arg_1)</p>
        <p>-- 当按位置传递参数时，如果结束参数是可选的，则只有结束参数可以省略。</p>
        <p>CALL catalog_name.system.procedure_name(arg_1, arg_2, ... arg_n)</p>
    </div>
    <h5>Ⅱ、快照管理</h5>
    <div class="icebergSqlArea">
        <p>-- // 查询表快照</p>
        <p>SELECT * FROM hadoop_prod.default.sample8.snapshots;</p>
        <p>-- （1）回滚到指定的快照id</p>
        <p>CALL hadoop_prod.system.rollback_to_snapshot('default.sample8', 3394509036086431205);</p>
        <p>-- （2）回滚到指定时间的快照</p>
        <p>CALL hadoop_prod.system.rollback_to_timestamp('default.sample8', TIMESTAMP '2023-05-29 19:58:00.008');</p>
        <p>-- （3）设置表的当前快照ID</p>
        <p>CALL hadoop_prod.system.set_current_snapshot('default.sample8', 6476372975407164130);</p>
        <p>-- （4）从快照变为当前表状态</p>
        <p>CALL hadoop_prod.system.cherrypick_snapshot('default.sample8', 6476372975407164130);</p>
        <p>-- 这个有问题，不要用这个写法切换快照</p>
        <p>CALL hadoop_prod.system.cherrypick_snapshot(snapshot_id => 3394509036086431205, table => 'default.sample8' );</p>
    </div>
    <h5>Ⅲ、元数据管理</h5>
    <div class="icebergSqlArea">
        <p>-- // 查询表快照</p>
        <p>SELECT * FROM hadoop_prod.default.sample8.snapshots;</p>

        <p>--（1）删除早于指定日期和时间的快照，但保留最近100个快照:</p>
        <p>CALL hadoop_prod.system.expire_snapshots('default.sample8', TIMESTAMP '2023-05-29 19:57:59.598', 100);</p>
        <p>--（2）删除Iceberg表中任何元数据文件中没有引用的文件</p>
        <p>--#列出所有需要删除的候选文件</p>
        <p>CALL hadoop_prod.system.remove_orphan_files(table => 'default.sample8', dry_run => true);</p>

        <p>--#删除指定catalog中db.sample表不知道的任何文件</p>
        <p>CALL hadoop_prod.system.remove_orphan_files(table => 'default.sample8', location => 'tablelocation/data');</p>
        <p>--（3）合并数据文件（合并小文件）</p>
        <p>CALL hadoop_prod.system.rewrite_data_files('default.sample8');</p>

        <p>CALL hadoop_prod.system.rewrite_data_files(table => 'default.sample8', strategy => 'sort', sort_order => 'id DESC NULLS LAST,name ASC NULLS FIRST');</p>

        <p>CALL hadoop_prod.system.rewrite_data_files(table => 'default.sample8', strategy => 'sort', sort_order => 'zorder(c1,c2)');</p>

        <p>CALL hadoop_prod.system.rewrite_data_files(table => 'default.sample8', options => map('min-input-files','2'));</p>

        <p>CALL hadoop_prod.system.rewrite_data_files(table => 'default.sample8', where => 'id = 3 and name = "foo"');</p>
        <p>--（4）重写表清单来优化执行计划</p>
        <p>CALL hadoop_prod.system.rewrite_manifests('default.sample8');</p>

        <p>--#重写表db中的清单。并禁用Spark缓存的使用。这样做可以避免执行程序上的内存问题。</p>
        <p>CALL hadoop_prod.system.rewrite_manifests('db.sample', false);</p>
    </div>
    <h5>Ⅳ、迁移表</h5>
    <div class="icebergSqlArea">
        <p>-- // 查询表快照</p>
        <p>SELECT * FROM hadoop_prod.default.sample8.snapshots;</p>

        <p>--（1）快照</p>
        <p>CALL hadoop_prod.system.snapshot('default.sample8','default.sample8_1');</p>
        <p>CALL hadoop_prod.system.snapshot('default.sample8', 'default.sample9', '/tmp/temptable/');</p>
        <p>--（2）迁移</p>
        <p>CALL hadoop_prod.system.migrate('hive_prod.default.sample8', map('foo', 'bar'))</p>
        <p>CALL hadoop_prod.system.migrate('default.sample8')</p>
        <p>--（3）添加数据文件</p>
        <p>CALL hive_prod.system.add_files(</p>
        <p>table => 'default.sample9',</p>
        <p v-html="`${source_table => 'default.sample8'}`"></p>
        <p>);</p>

        <p>CALL hadoop_prod.system.add_files(</p>
        <p>table => 'db.tbl',</p>
        <p v-html="`${source_table => '`parquet`.`path/to/table`'}`"></p>
        <p>)</p>
    </div>
    <h5>Ⅴ、元数据信息</h5>
    <div class="icebergSqlArea">
        <p>--（1）获取指定快照的父快照id</p>
        <p>CALL hadoop_prodhadoop_prodhadoop_prod.system.ancestors_of('default.sample8')</p>
        <p>--（2）获取指定快照的所有祖先快照</p>
        <p>CALL hadoop_prod.system.ancestors_of('default.sample8', 3394509036086431205);</p>
        <p>CALL hadoop_prod.system.ancestors_of(snapshot_id => 3394509036086431205, table => 'default.sample8')</p>
    </div>
    <h1> flink引擎</h1>
    <h2>一、flinkSql常规操作</h2>
    <h3>1、catalog操作</h3>
    <div class="icebergSqlArea">
        <p>-- hive catalog</p>
        <p>CREATE CATALOG hadoop_catalog1 WITH (</p>
        <p>'type'='iceberg',</p>
        <p>'catalog-type'='hadoop',</p>
        <p>'warehouse'='hdfs://node181:8020/iceberg',</p>
        <p>'property-version'='1'</p>
        <p>);</p>

        <p>-- hadoop catalog</p>
        <p>CREATE CATALOG hive_catalog1 WITH (</p>
        <p>'type'='iceberg',</p>
        <p>'catalog-type'='hive',</p>
        <p>'uri'='thrift://node86:9083',</p>
        <p>'clients'='5',</p>
        <p>'property-version'='1',</p>
        <p>'warehouse'='hdfs://node181:8020/user/hive/warehouse'</p>
        <p>);</p>

        <p>-- 查看catalog</p>
        <p>show catalogs;</p>

        <p>-- 使用catalog</p>
        <p>use catalog hive_catalog;</p>
    </div>
    <h3> 2、DDL语句</h3>
    <h4> 1）创建数据库</h4>
    <div class="icebergSqlArea">
        <p>-- 创建库</p>
        <p>create database flinkdb;</p>
        <p>-- 查询库</p>
        <p>show databases;</p>
        <p>-- 切换库</p>
        <p>use flinkdb;</p>
    </div>
    <h4> 2）创建表</h4>
    <div class="icebergSqlArea">
        <p>-- 基础表</p>
        <p>CREATE TABLE `hadoop_catalog`.`flinkdb`.`sample` (</p>
        <p>    id BIGINT COMMENT 'unique id',</p>
        <p>    data STRING</p>
        <p>);</p>
        <p>-- 分区表</p>
        <p>CREATE TABLE `hive_catalog`.`flinkdb`.`sample` (</p>
        <p>    id BIGINT COMMENT 'unique id',</p>
        <p>    data STRING</p>
        <p>) PARTITIONED BY (data);</p>
        <p>-- 复制结构建表</p>
        <p>CREATE TABLE  `hive_catalog`.`flinkdb`.`sample_like` LIKE `hive_catalog`.`flinkdb`.`sample`;</p>
        <p>--建表指定catalog</p>
        <p>CREATE TABLE flink_table (</p>
        <p>    id   BIGINT,</p>
        <p>    data STRING</p>
        <p>) WITH (</p>
        <p>    'connector'='iceberg',</p>
        <p>    'catalog-name'='hive_prod',</p>
        <p>    'uri'='thrift://node86:9083',</p>
        <p>    'warehouse'='hdfs://node181:8020/user/hive/warehouse'</p>
        <p>);</p>

        <p>--支持upsert命令的表结构</p>
        <p>CREATE TABLE `hive_catalog`.`flinkdb`.`flink2` (</p>
        <p>`id`  INT UNIQUE COMMENT 'unique id',</p>
        <p>`data` STRING NOT NULL,</p>
        <p>PRIMARY KEY(`id`) NOT ENFORCED</p>
        <p>) with ('format-version'='2', 'write.upsert.enabled'='true');</p>
    </div>
    <h4>3）修改表</h4>
    <div class="icebergSqlArea">
        <p>-- 1）修改表属性</p>
        <p>ALTER TABLE `hive_catalog`.`flinkdb`.`sample` SET ('write.format.default'='avro');</p>
        <p>-- 2）修改表名 hadoop catalog不支持修改表名</p>
        <p>ALTER TABLE `hive_catalog`.`flinkdb`.`sample` RENAME TO `hive_catalog`.`default`.`new_sample`;</p>
    </div>
    <h4> 4）删除表</h4>
    <div class="icebergSqlArea">
        <p>DROP TABLE `hive_catalog`.`flinkdb`.`new_sample`;</p>
    </div>
    <h3> 3、DML语句</h3>
    <h4> 1）插入语句</h4>
    <div class="icebergSqlArea">
        <p>-- 基础插入</p>
        <p>INSERT INTO `hive_catalog`.`flinkdb`.`sample` VALUES (1, 'a');</p>
        <p>-- 表查询结果插入</p>
        <p>INSERT INTO `hive_catalog`.`flinkdb`.`sample_like` SELECT id, data from `hive_catalog`.`flinkdb`.`sample`;</p>
        <p>-- 基础批量插入数据</p>
        <p>INSERT INTO flink_table VALUES (1, 'AAA'), (2, 'BBB'), (3, 'CCC');</p>

        <p>--不可用：报错无界数据流不支持overwrite</p>
        <p>INSERT OVERWRITE flink_table VALUES (5, 'a');</p>

        <p>-- upsert(没看出upsert)</p>
        <p>INSERT INTO flink2 /*+ OPTIONS('upsert-enabled'='true') */ values(1,'e');</p>
    </div>
    <h4> 2）查询语句</h4>
    <div class="icebergSqlArea">
        <p>-- 表结构返回查询结果</p>
        <p>SET execution.result-mode=tableau;</p>
        <p>SELECT * FROM hive_catalog.flinkdb.flink2;</p>


        <p>-- 批量读</p>
        <p>SET execution.runtime-mode = batch;</p>
        <p>SELECT * FROM hive_catalog.flinkdb.flink2;</p>

        <p>-- 流式读：任务在yarn-session客户端上不会中止</p>
        <p>SET execution.runtime-mode = streaming;</p>
        <p>SET table.dynamic-table-options.enabled=true;</p>
        <p>SELECT * FROM hive_catalog.flinkdb.flink2 /* +options('streaming'='true','minitor-interval'='1s') */;</p>
    </div>
</div>
</template>
<script>
export default {
  name: '',
  components: {},
  data() {
    return {
    }
  },
  computed: {},
  watch: {},
  created() {},
  methods: {}
}
</script>
<style scoped lang="scss">
.icebergArea {
    height: 600px;
    overflow: auto;
    padding: 10px;
    .icebergSqlArea {
        border: 1px solid #f1f1f1;
        padding: 10px;
        background: #f8f8f8;
        border-radius: 4px;
    }
}
</style>
