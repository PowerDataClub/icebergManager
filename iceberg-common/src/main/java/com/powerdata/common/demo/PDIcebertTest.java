package com.powerdata.common.demo;

import com.powerdata.common.utils.iceberg.PDHdfsUtils;
import com.powerdata.common.utils.iceberg.PDIcebergFlinkUtils;
import com.powerdata.common.utils.iceberg.PDIcebergUtils;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.hive.HiveCatalog;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class PDIcebertTest {
    @Test
    public void test1() throws Exception {
//        PDIcebergUtils pdIcebergUtils = PDIcebergUtils.build("hadoop","","hdfs://128.12.125.56:9000/user/iceberg/warehouse");
//        pdIcebergUtils.renameTable("iceberg_db","test11","aaa","test11");
//        pdIcebergUtils.getTransactionMessage("iceberg_db", "java_hdfs_table112");
//        pdIcebergUtils.addDatabases("iceberg_db");
//        pdIcebergUtils.deleteDatabases("icebergdb1");
//        for (String listDataBase : pdIcebergUtils.listDataBases()) {
//            System.out.println(listDataBase);
//        }
//        for (TablePartitionKeysBean tablePartitionKeysBean : pdIcebergUtils.getPartitionMessage("iceberg_db", "java_hdfs_table4")) {
//            System.out.println(tablePartitionKeysBean);
//        }
//        for (TableTransactionsBean tableTransactionsBean : pdIcebergUtils.getTransactionsMessage("iceberg_db", "java_hdfs_table4")) {
//            System.out.println(tableTransactionsBean);
//        }
//        System.out.println(pdIcebergUtils.getTableMetrics("iceberg_db", "java_hdfs_table3"));
//        for (TableTransactionsBean tableTransactionsBean : pdIcebergUtils.getTransactionsMessage("iceberg_db", "java_hdfs_table4")) {
//            System.out.println(tableTransactionsBean);
//        }
//        long time = DateUtil.parse("2023-06-08 14:02:21", "yyyy-MM-dd HH:mm:ss").getTime();
//        for (Map<String, Object> tableDatum : pdIcebergUtils.getTableDataBeforeTime("iceberg_db", "java_hdfs_table4",time)) {
//            System.out.println(tableDatum.toString());
//        }
//        pdIcebergUtils.getTransactionMessage("iceberg_db", "java_hdfs_table4");
//        pdIcebergUtils.getTableData("iceberg_db", "test3",1,100);
//        ArrayList<String> addData = new ArrayList<>();
//        addData.add("1,dinghao,test1");
//        pdIcebergUtils.addTableData("iceberg_db", "java_hdfs_table110",addData,",");
//        ArrayList<HashMap<String, String>> addData = new ArrayList<>();
//        HashMap<String, String> map = new HashMap<>();
//        for(int i=0;i<100000;i++){
//            map = new HashMap<>();
//            map.put("id",i+"");
//            map.put("name","dh");
//            map.put("age",i%10+"");
//            addData.add(map);
//        }
//        System.out.println("test3");
//        System.out.println(new Date());
//        pdIcebergUtils.addTableData("iceberg_db", "test3",addData);
//        System.out.println(new Date());
//        System.out.println("test2");
//        System.out.println(new Date());
//        pdIcebergUtils.addTableData("iceberg_db", "test2",addData);
//        System.out.println(new Date());
//        System.out.println(pdIcebergUtils.getTableMetrics("iceberg_db", "test12"));
//        pdIcebergUtils.rollbackSnapshot("iceberg_db", "java_hdfs_table4",8827322425413741527L);
//        pdIcebergUtils.cherryPickSnapshot("iceberg_db", "java_hdfs_table4",8827322425413741527L);
//        for (Map.Entry<String, Object> stringObjectEntry : pdIcebergUtils.getTableData("iceberg_db", "java_hdfs_table5",6116129058367772000L, 10, 2).entrySet()) {
//            System.out.println(stringObjectEntry.getKey()+"->"+stringObjectEntry.getValue().toString());
//        }
//        for (TableTransactionsBean tableTransactionsBean : pdIcebergUtils.getTransactionsMessage("iceberg_db", "java_hdfs_table5")) {
//            System.out.println(tableTransactionsBean);
//        }
    }
    @Test
    public void test2() throws Exception {
//        PDIcebergUtils pdIcebergUtils =
//                PDIcebergUtils.build("hadoop1","hive","thrift://128.12.125.59:9083","hdfs://128.12.125.81:9000/user/iceberg/warehouse2","hive");
//        pdIcebergUtils.addDatabases("testhive");
    }

    @Test
    public void test3(){
        HiveCatalog hiveCatalog = new HiveCatalog();
//        final Configuration configuration = new Configuration();
//        configuration.addResource(new Path("D:\\1gitpro\\ruoyi\\IcebergManager\\iceberg-admin\\src\\main\\resources\\hive-site.xml"));
//        configuration.addResource(new Path("D:\\1gitpro\\ruoyi\\IcebergManager\\iceberg-admin\\src\\main\\resources\\core-site.xml"));
//        configuration.addResource(new Path("D:\\1gitpro\\ruoyi\\IcebergManager\\iceberg-admin\\src\\main\\resources\\hdfs-site.xml"));
//
//        hiveCatalog.setConf(configuration);
        HashMap<String, String> map = new HashMap<>();
        map.put(CatalogProperties.WAREHOUSE_LOCATION,"hdfs://dh63:9000/user/hive/warehouse");
        map.put(CatalogProperties.URI,"thrift://node59:9083");
        hiveCatalog.initialize("hive",map);
        for (Namespace listNamespace : hiveCatalog.listNamespaces()) {
            System.out.println(listNamespace.toString());
        }
    }

    @Test
    public void test5() throws Exception {
//        PDIcebergUtils pdIcebergUtils =
//                PDIcebergUtils.build("hadoop1","hadoop","thrift://128.12.125.56:9083",
//                        "hdfs://128.12.125.56:9000/user/iceberg/warehouse2");
//        pdIcebergUtils.mergeSmallFile("test1","a2");
//        System.out.println(pdIcebergUtils.getTableMetrics("test2", "t5").toString());
//        pdIcebergUtils.getCreateTableSql("test2","t2");
//        Table table = pdIcebergUtils.getTable(TableIdentifier.of(Namespace.of("test2"), "cc"));
//        final PDIcebergSparkUtils pdIcebergSparkUtils =
//                PDIcebergSparkUtils.build("hadoop1",
//                        "hadoop",
//                        "thrift://128.12.125.56:9083",
//                        "hdfs://128.12.125.56:9000/user/iceberg/warehouse2",
//                        "test2");
//
//        pdIcebergSparkUtils.mergeFile(table);
//        pdIcebergUtils.expireOldSnapshots(table.currentSnapshot().timestampMillis()-1,table);
//        System.out.println("OK");
//        pdIcebergSparkUtils.deleteOldFiles(table);

        PDHdfsUtils.catAvroFile(
                "hdfs://128.12.125.56:9000/user/iceberg/warehouse2/test1/dh11/metadata/snap-6858285610403569772-1-74a6c3cf-cd01-46c7-8b03-d23c2ffda76a.avro");
    }

    @Test
    public void test6() throws Exception{
        PDIcebergFlinkUtils build = PDIcebergFlinkUtils.build("hadoopCatalog", "hadoop", "", "hdfs://172.29.0.49:9000/user/iceberg/warehouse", "","hive");
        build.executeSql("use catalog hadoopCatalog");
        build.executeSql("use iceberg_db");
//        build.executeSql("SET execution.result-mode=tableau");
//        select * from java_hdfs_table5 limit 5
        for (Map<String, Object> stringObjectMap :build.executeSql("show tables")) {
            for (Map.Entry<String, Object> stringObjectEntry : stringObjectMap.entrySet()) {
                System.out.println(stringObjectEntry.getKey()+"->"+stringObjectEntry.getValue());
            }
        }
        for (Map<String, Object> stringObjectMap :build.executeSql("select * from java_hdfs_table5")) {
            for (Map.Entry<String, Object> stringObjectEntry : stringObjectMap.entrySet()) {
                System.out.println(stringObjectEntry.getKey()+"->"+stringObjectEntry.getValue());
            }
        }
    }
}
