package com.powerdata.common.demo;

import com.powerdata.common.utils.iceberg.PDIcebergSparkUtils;
import org.apache.curator.shaded.com.google.common.collect.ImmutableList;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.io.*;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;

import java.util.*;

public class CatalogTest2 {
    //获取元数据信息
    public static void main(String[] args) {
        HiveCatalog hiveCatalog = new HiveCatalog();
        hiveCatalog.setConf(new Configuration());
        HashMap<String, String> properties = new HashMap<>();
        properties.put("warehouse", "hdfs://node49:8020/user/hive/warehouse");
        properties.put("uri", "thrift://node86:9083");
        properties.put("clients", "2");    // 客户端连接池大小
        hiveCatalog.initialize("hive_catalog", properties);
        for (Namespace namespace : hiveCatalog.listNamespaces()) {
            if(!namespace.toString().equals("flinkdb")){
                continue;
            }
            System.out.println("================"+namespace+"==============");
            for (Map.Entry<String, String> entry : hiveCatalog.loadNamespaceMetadata(namespace).entrySet()) {
                System.out.println(entry.getKey()+"->"+entry.getValue());
            }
            for (TableIdentifier table : hiveCatalog.listTables(namespace)) {
                System.out.println("------------"+table.name()+"-------------");
                try{
                    Table table1 = hiveCatalog.loadTable(table);
                    Schema schema = table1.schema();
                    System.out.println(schema);
                    TableScan tableScan = table1.newScan();
                    for (Types.NestedField column : tableScan.schema().columns()) {
                        System.out.println(column.name() + "->" + column.type());
                    }
                    System.out.println(tableScan.select("id"));
                }catch (Exception e){
                    System.out.println(e.getMessage());
                }

            }
        }
    }
    //hivecatalog的增删改查
    @Test
    public void Hivecatalogtest1(){
        HiveCatalog hiveCatalog = new HiveCatalog();
        hiveCatalog.setConf(new Configuration());
        HashMap<String, String> properties = new HashMap<>();
        properties.put("warehouse", "hdfs://node49:8020/user/hive/warehouse");
        properties.put("uri", "thrift://node86:9083");
        properties.put("clients", "2");    // 客户端连接池大小
        hiveCatalog.initialize("hive_catalog", properties);
        //创建表
        Schema schema = new Schema(
                Types.NestedField.required(1, "user_name", Types.StringType.get()),
                Types.NestedField.required(2, "order_time", Types.TimestampType.withZone()),
                Types.NestedField.optional(3, "buy_products", Types.ListType.ofRequired(4, Types.StringType.get()))
        );

        PartitionSpec partitionSpec = PartitionSpec.builderFor(schema)
                // 从timestamp类型字段，解析int类型的小时作为分区字段
                .hour("order_time")
                // 直接取表字段作为分区字段
                .identity("user_name")
                .build();

        TableIdentifier tableName = TableIdentifier.of("flinkdb", "java_hive_table");
        //新增表
//        Table table = hiveCatalog.createTable(tableName, schema, partitionSpec);
        //加载表
//        Table table1 = hiveCatalog.loadTable(tableName);
        //重命名表
        hiveCatalog.renameTable(tableName, TableIdentifier.of("flinkdb","java_hive_table2"));
        //删除表
//        hiveCatalog.dropTable(tableName,true);
    }

    @Test
    public void HadoopCatalogTest1(){

        String warehousePath = "hdfs://node49:8020/user/iceberg/warehouse";
        HadoopCatalog hadoopCatalog = new HadoopCatalog(new Configuration(), warehousePath);

        // =============创建表==================
        Types.NestedField hobby = Types.NestedField.optional(3, "hobby", Types.ListType.ofRequired(4, Types.StringType.get()));
        Schema schema = new Schema(
                // 通过Java API生成的Schema，需要给每个字段指定唯一ID
                Types.NestedField.required(1, "user_name", Types.StringType.get()),
                Types.NestedField.required(2, "order_time", Types.TimestampType.withZone()),
                hobby
        );

        PartitionSpec partitionSpec = PartitionSpec.builderFor(schema)
                // 从timestamp类型字段，解析int类型的小时作为分区字段
//                .hour("order_time")
                // 直接取表字段作为分区字段
                .identity("user_name")
                .build();

        // 参数分别是数据库名和表名
        TableIdentifier tableName = TableIdentifier.of("iceberg_db", "java_hdfs_table4");
        Table table = hadoopCatalog.createTable(tableName, schema, partitionSpec);


        // =============加载一个已经存在的表=========
//        Table table1 = hadoopCatalog.loadTable(tableName);


        // =============重命名表=========
        //hadoopCatalog.renameTable(tableName, TableIdentifier.of("iceberg_db", "java_hadoop_table2"));


        // =============删除表=========
         hadoopCatalog.dropTable(tableName, true);
    }

    @Test
    public void HadoopCatalogTest2(){
        // =======初始化Hadoop Tables=============
        Configuration configuration = new Configuration();
        HadoopTables hadoopTables = new HadoopTables(configuration);

        // =============创建表==================
        Schema schema = new Schema(
                // 通过Java API生成的Schema，需要给每个字段指定唯一ID
                Types.NestedField.optional(1, "id", Types.IntegerType.get(),"用户id"),
                Types.NestedField.optional(2, "name", Types.StringType.get(),"用户名"),
                Types.NestedField.optional(3, "birth", Types.StringType.get())
        );

        PartitionSpec partitionSpec = PartitionSpec.builderFor(schema)
                // 从timestamp类型字段，解析int类型的小时作为分区字段
                // 直接取表字段作为分区字段
                .identity("name")
//                .bucket("id",3)
                .build();

        String warehouseTablePath = "hdfs://128.12.125.56:8020/user/iceberg/warehouse/iceberg_db/java_hdfs_table113";
        Table table = hadoopTables.create(schema, partitionSpec, warehouseTablePath);


        // =============加载一个已经存在的表=========
//        Table load = hadoopTables.load(warehouseTablePath);

        // =============删除表=========
//        hadoopTables.dropTable(warehouseTablePath,true);
    }

    @Test
    public void MetadataAPItest1(){
        // =======初始化Hadoop Catalog=============
        String warehousePath = "hdfs://node49:8020/user/iceberg/warehouse";
        HadoopCatalog hadoopCatalog = new HadoopCatalog(new Configuration(), warehousePath);

        // =============加载一个已经存在的表=========
        // 参数分别是数据库名和表名
        TableIdentifier tableName = TableIdentifier.of("iceberg_db", "java_hdfs_table4");
        Table table  = hadoopCatalog.loadTable(tableName);

        // ==============表metadata==================
        System.out.println("返回表的Schema");
        Schema schema  = table.schema();
        System.out.println(schema.toString());

        // 返回表的PartitionSpec
        System.out.println("返回表的PartitionSpec");
        PartitionSpec partitionSpec = table.spec();
        System.out.println(partitionSpec.toString());

        // 返回map形式的key:value属性，本示例返回结果为：{write.format.default=parquet, write.parquet.compression-codec=gzip}
        System.out.println("返回map形式的key:value属性，本示例返回结果为：{write.format.default=parquet, write.parquet.compression-codec=gzip}");
        Map<String, String> properties = table.properties();
        for (Map.Entry<String, String> stringStringEntry : properties.entrySet()) {
            System.out.println(stringStringEntry.getKey()+"->"+stringStringEntry.getValue());
        }

        // 返回表当前的Snapshot
        System.out.println("返回表当前的Snapshot");
        Snapshot currentSnapshot  = table.currentSnapshot();
//        System.out.println(currentSnapshot.snapshotId());

        // 根据snapshot id返回对应的Snapshot
        System.out.println("根据snapshot id返回对应的Snapshot");
        Snapshot defineSanpshot  = table.snapshot(7067373115111862895L);

        // 返回表的所有Snapshot
        System.out.println("返回表的所有Snapshot");
        Iterable<Snapshot> snapshots = table.snapshots();
        Iterator<Snapshot> iterator = snapshots.iterator();
        while (iterator.hasNext()) {
            System.out.println(iterator.next().snapshotId());
        }

        // 返回表在HDFS上的路径，本示例结果为：hdfs://nnha/user/iceberg/warehouse/iceberg_db/my_user
        System.out.println("返回表在HDFS上的路径，本示例结果为：hdfs://nnha/user/iceberg/warehouse/iceberg_db/my_user");
        String location = table.location();
        System.out.println(location);

        // 将表更新到最新的version
        table.refresh();

        // 使用FileIO读写table files
        System.out.println("使用FileIO读写table files");
        FileIO fileIO = table.io();
        System.out.println(fileIO.newOutputFile("test1"));

        // 使用LocationProvider，为data和metadata files创建path
        System.out.println("使用LocationProvider，为data和metadata files创建path");
        LocationProvider locationProvider = table.locationProvider();
        System.out.println(locationProvider);
    }

    @Test
    public void FileLevelTest1(){
        // =======初始化Hadoop Catalog=============
        String warehousePath = "hdfs://node49:8020/user/iceberg/warehouse";
        HadoopCatalog hadoopCatalog = new HadoopCatalog(new Configuration(), warehousePath);

        // =============加载一个已经存在的表=========
        // 参数分别是数据库名和表名
        TableIdentifier tableName = TableIdentifier.of("iceberg_db", "java_hdfs_table3");
        Table table = hadoopCatalog.loadTable(tableName);


        // ==============表Scanning==================
        // TableScan是一个不可变的对象
        TableScan tableScan =
                table.newScan()
//                        .filter(Expressions.equal("user_id", 2))
                        .select("user_id", "user_name");
        // .asOfTime(timestampMillis:Long)    // 从指定时间戳开始读取数据
        // .useSnapshot(snapshotId:Long)         // 从指定snapshot id开始读取数据

        // 返回files
        CloseableIterable<FileScanTask> fileScanTasks = tableScan.planFiles();
        CloseableIterator<FileScanTask> iterator = fileScanTasks.iterator();
        while (iterator.hasNext()) {
            System.out.println(iterator.next());
        }

        // 返回tasks
        CloseableIterable<CombinedScanTask> combinedScanTasks = tableScan.planTasks();
        CloseableIterator<CombinedScanTask> iterator1 = combinedScanTasks.iterator();
        while (iterator1.hasNext()) {
            System.out.println(iterator1.next());
        }

        // 返回读projection
        System.out.println(tableScan.schema());
    }

    @Test
    public void RowLevelTest1(){
        // =======初始化Hadoop Catalog=============
        String warehousePath = "hdfs://node49:8020/user/iceberg/warehouse";
        HadoopCatalog hadoopCatalog = new HadoopCatalog(new Configuration(), warehousePath);

        // =============加载一个已经存在的表=========
        // 参数分别是数据库名和表名
        TableIdentifier tableName = TableIdentifier.of("iceberg_db", "java_hdfs_table3");
        Table table = hadoopCatalog.loadTable(tableName);


        // ==============表Scanning==================
        IcebergGenerics.ScanBuilder scanBuilder = IcebergGenerics.read(table);

        CloseableIterator<Record> iterator = scanBuilder.where(Expressions.equal("user_name", "2"))
                .select("user_name","order_time")
                .build().iterator();
        while (iterator.hasNext()) {
            System.out.println(iterator.next().toString());
        }
    }

    @Test
    public void UpdateTest1(){
        // =======初始化Hadoop Catalog=============
        String warehousePath = "hdfs://node49:8020/user/iceberg/warehouse";
        HadoopCatalog hadoopCatalog = new HadoopCatalog(new Configuration(), warehousePath);

        // =============加载一个已经存在的表=========
        // 参数分别是数据库名和表名
        TableIdentifier tableName = TableIdentifier.of("iceberg_db", "java_hdfs_table3");
        Table table = hadoopCatalog.loadTable(tableName);


        // ==============表update操作==================

        // 更新表的schema
        table.updateSchema()
                .addColumn("age", Types.IntegerType.get());
        // .commit()

        // 更新表的properties属性
        UpdateProperties updateProperties = table.updateProperties();

        // 更新表的根路径
        UpdateLocation updateLocation = table.updateLocation();

        // 添加data files到表
        AppendFiles appendFiles = table.newAppend();


        // 添加data files到表, 但不会compact metadata
        AppendFiles fastAppendFiles = table.newFastAppend();

        // 添加data files到表, 且删除被覆盖的files
        OverwriteFiles overwriteFiles = table.newOverwrite();

        // 删除data files
        DeleteFiles deleteFiles = table.newDelete();

        // rewrite data files, 用new versions替换已经存在的files
        RewriteFiles rewriteFiles = table.newRewrite();

        // 创建一个新的表级别事务
        Transaction transaction = table.newTransaction();

        // 为了更快的scan planning，用clustering files重写manifest
        RewriteManifests rewriteManifests = table.rewriteManifests();

        // 对表snapshot进行管理，比如将表state回退到某个snapshot id
        ManageSnapshots manageSnapshots = table.manageSnapshots();
    }

    @Test
    public void TransactionsTest(){
        // =======初始化Hadoop Catalog=============
        String warehousePath = "hdfs://node49:8020/user/iceberg/warehouse";
        HadoopCatalog hadoopCatalog = new HadoopCatalog(new Configuration(), warehousePath);

        // =============加载一个已经存在的表=========
        // 参数分别是数据库名和表名
        TableIdentifier tableName = TableIdentifier.of("iceberg_db", "java_hdfs_table3");
        Table table = hadoopCatalog.loadTable(tableName);


        // ==============Transactions==================
        Transaction transaction = table.newTransaction();

        // 提交一个delete操作到Transaction
        transaction
                .newDelete()
                .deleteFromRowFilter(Expressions.equal("user_id", 2));
        // .commit()

        // transaction.newAppend().appendFile(DataFile).commit();

        // 提交所有操作到表
        // transaction.commitTransaction();
    }

    @Test
    public void InsertDataTest() throws Exception {
        // =======初始化Hadoop Catalog=============
        String warehousePath = "hdfs://128.12.125.56:8020/user/iceberg/warehouse";
        HadoopCatalog hadoopCatalog = new HadoopCatalog(new Configuration(), warehousePath);


// 定义表结构schema


        TableIdentifier tableName = TableIdentifier.of("iceberg_db", "java_hdfs_table110");

//        PartitionSpec spec = PartitionSpec.builderFor(schema).bucket("name",3).build();
//        //新增表
//        Table table = hadoopCatalog.createTable(tableName, schema, spec);
        Table table = hadoopCatalog.loadTable(tableName);
        Schema schema = table.schema();
                // =============加载一个已经存在的表=========
        // 参数分别是数据库名和表名


        GenericRecord record = GenericRecord.create(schema);
        record.setField("id",1);
        record.setField("name","chen");
        record.setField("birth","2020-03-08");
        ImmutableList.Builder<GenericRecord> builder = ImmutableList.builder();
        builder.add(record);
//        builder.add(ImmutableMap.of("id", 1, "name", "chen", "birth", "2020-03-08"));
//        builder.add(ImmutableMap.of("id", 2, "name", "yuan", "birth", "2021-03-09"));
//        builder.add(ImmutableMap.of("id", 3, "name", "jie", "birth", "2023-03-10"));
//        builder.add(ImmutableMap.of("id", 4, "name", "ma", "birth", "2023-03-11"));
//        ImmutableList<GenericRecord> records = builder.build();


// 2. 将记录写入parquet文件
        String filepath = table.location() + "/" + UUID.randomUUID().toString();
        OutputFile file = table.io().newOutputFile(filepath);
        DataWriter<GenericRecord> dataWriter =
                Parquet.writeData(file)
                        .schema(schema)
                        .createWriterFunc(GenericParquetWriter::buildWriter)
                        .overwrite()
                        .withSpec(PartitionSpec.unpartitioned())
                        .build();
        try {
            for (GenericRecord record1 : builder.build()) {
                dataWriter.write(record1);
            }
        } finally {
            dataWriter.close();
        }

// 3. 将文件写入table中
        DataFile dataFile = dataWriter.toDataFile();
        table.newAppend().appendFile(dataFile).commit();
    }

    @Test
    public void SparkTest(){
        SparkSession spark = SparkSession.builder().master("local[*]").appName("test")  //指定hadoop catalog，catalog名称为hadoop_prod
                .config("spark.sql.catalog.iceberg_db", "org.apache.iceberg.spark.SparkCatalog")
                .config("spark.sql.catalog.iceberg_db.type", "hadoop")
                .config("spark.sql.catalog.iceberg_db.warehouse", "hdfs://128.12.125.56:8020/user/iceberg/warehouse/iceberg_db")
                .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                .config("spark.sql.warehouse.dir", "hdfs://128.12.125.56:8020/user/iceberg")
                .getOrCreate();
        long time = new Date().getTime();
//        CREATE TABLE iceberg_db.test1 (
//                id bigint,
//                name string);
//        INSERT INTO iceberg_db.test1 VALUES (1, 'zhangsan'), (2, 'lisi'), (3, 'wangwu');
//        update iceberg_db.test1 set name='wangwu' where id=1;
//        delete from iceberg_db.test1 where name = 'wangwu';
//        select * from iceberg_db.test1;
//        spark.sql("create database aa");
//        spark.sql("show databases").show();
//        spark.sql("use database aa.db");
//        spark.sql("show tables").show();
//        spark.sql("drop table iceberg_db.java_hdfs_table5");
//        spark.sql("insert into iceberg_db.test12 values (10,'aa'),(11,'lisaai')").show();
//        spark.sql("update iceberg_db.test12 set name='wangwu' where id=10");
//        spark.sql("delete from iceberg_db.test12 where name = 'aa'");
        Dataset<Row> sql = spark.sql("select * from iceberg_db.test12");
        String[] columns = sql.columns();
        System.out.println(Arrays.asList(columns).toString());
        List<Row> collect = sql.toJavaRDD().collect();
        for (Row row : collect) {
            for (String column : columns) {
                System.out.println(column+"->"+row.getAs(column));
            }
        }
    }

    @Test
    public void sparkClientTest(){
        for (Map<String, Object> stringObjectMap :
                PDIcebergSparkUtils.build("aaa","hadoop", "", "hdfs://128.12.125.56:8020/user/iceberg/warehouse","iceberg_db","hive")
                .executeSql("select * from iceberg_db.test11")) {
            System.out.println(stringObjectMap);
        }
    }
}
