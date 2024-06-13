package com.powerdata.common.utils.iceberg;

import org.apache.commons.lang3.ObjectUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.spark.actions.SparkActions;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.deploy.SparkSubmit;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;

import java.util.*;

/**
 * @author dinghao
 * @version 1.0
 * @description
 * @date 2023/6/16 9:50
 */
public class PDIcebergSparkUtils extends PDIcebergExecUtils {

    private static final Logger log = LoggerFactory.getLogger(PDIcebergSparkUtils.class);
    private String execMem = "1G";
    private String execCores = "1";
//    private String uploadHiveFilePath = "./opt/icebergManager/hiveconf/";
    private String uploadHiveFilePath = "D:\\";
    private String catalogId;
    private SparkSession sparkSession;
    private String execUser;
    private static SparkContext sparkContext;
    public static HashMap<String, PDIcebergSparkUtils> icebergSparkClientsMap=new HashMap<>();
    private PDIcebergSparkUtils(String catalogId, String type, String hiveUri, String hdfsUri,
                                String databaseName,String hadoopUser){
        execUser = hadoopUser;
        System.setProperty("HADOOP_USER_NAME", hadoopUser);
        if("hive".equals(type)) {
            sparkSession = SparkSession.builder()
                    .master("local[*]")
                    .appName(type + "_sqlClient")
                    .config(new SparkConf().set("spark.executor.memory",execMem)
                            .set("spark.executor.cores",execCores)
                            .set("spark.executor.nums","8")
                            .set("spark.default.parallelism","8"))
                    .config("spark.sql.catalog." + catalogId, "org.apache.iceberg.spark.SparkCatalog")
                    .config("spark.sql.catalog." + catalogId + ".type", type)
                    .config("spark.sql.catalog." + catalogId + ".uri", hiveUri)
                    .config("spark.sql.parquet.enableVectorizedRead","true")
                    .getOrCreate();
            sparkContext = sparkSession.sparkContext();
            Configuration configuration = sparkContext.hadoopConfiguration();
            String dir = uploadHiveFilePath+catalogId+"/";
            configuration.addResource(new Path(dir+"core-site.xml"));
            configuration.addResource(new Path(dir+"hdfs-site.xml"));
            configuration.addResource(new Path(dir+"hive-site.xml"));
        }else {
//            spark://172.29.0.86:7077
            sparkSession = SparkSession.builder()
                    .master("local[*]")
//                    .master("spark://node181:7077")
                    .appName(type + "_sqlClient")
                    .config(new SparkConf().set("spark.executor.memory",execMem)
                            .set("spark.executor.cores",execCores)
                            .set("spark.executor.nums","8")
                            .set("spark.default.parallelism","8")
//                            .set("spark.driver.host", "ToolsOnKeys")
                            )
                    .config("spark.sql.catalog." + catalogId, "org.apache.iceberg.spark.SparkCatalog")
                    .config("spark.sql.catalog." + catalogId + ".type", type)
                    .config("spark.sql.catalog." + catalogId + ".warehouse", hdfsUri)
                    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                    .config("spark.sql.warehouse.dir", hdfsUri)
                    .config("spark.sql.parquet.enableVectorizedRead","true")
                    .getOrCreate();
            sparkContext = sparkSession.sparkContext();
        }
        this.catalogId = catalogId;
    }

    //初始化PDIcebergSparkUtils对象
    public static PDIcebergSparkUtils build(String catalogId, String type, String hiveUri, String hdfsUri,
                                            String databaseName,String hadoopUser){
        PDIcebergSparkUtils pdIcebergSparkUtils = icebergSparkClientsMap.get(catalogId);
        if(ObjectUtils.isEmpty(pdIcebergSparkUtils)){
            while(ObjectUtils.isNotEmpty(sparkContext) && sparkContext.isStopped()){
                sparkContext.stop();
                if(sparkContext.isStopped()){
                    sparkContext = null;
                }
            }
            sparkContext = null;
            icebergSparkClientsMap.clear();
            icebergSparkClientsMap.put(catalogId,new PDIcebergSparkUtils(catalogId,type,hiveUri,hdfsUri,databaseName,hadoopUser));
        }
       return icebergSparkClientsMap.get(catalogId);
    }

    @Override
    public List<Map<String,Object>> executeSql(String sql){
        ArrayList<Map<String, Object>> results = new ArrayList<>();
        sql = sql.trim();
        if(sql.toLowerCase().startsWith("select") && !sql.contains("limit")){
            sql = sql +" limit 200 ";
        }
        if(sql.getBytes().length < 10*1024*1024){
            log.info("执行sql："+sql);
        }
        Dataset<Row> rowDataset = sparkSession.sql(sql);
        String[] columns = rowDataset.columns();
        List<Row> rows = rowDataset.collectAsList();
        for (Row row : rows) {
            LinkedHashMap<String, Object> resultMap = new LinkedHashMap<>();
            for (String column : columns) {
                resultMap.put(column,row.getAs(column));
            }
            results.add(resultMap);
        }
        if(results.size()==0 && ObjectUtils.isNotEmpty(columns) && columns.length!=0){
            LinkedHashMap<String, Object> resultMap = new LinkedHashMap<>();
            for (String column : columns) {
                resultMap.put(column,null);
            }
            results.add(resultMap);
        }
        return results;
    }

    @Override
    public List<Map<String,Object>> executeSql(String sql,String flag){
        ArrayList<Map<String, Object>> results = new ArrayList<>();
        sql = sql.trim();
        if(sql.toLowerCase().startsWith("select") && !sql.contains("limit")){
            sql = sql +" limit 200 ";
        }
        sparkSession.log().info("begin"+flag);
        Dataset<Row> rowDataset = sparkSession.sql(sql);
        String[] columns = rowDataset.columns();
        List<Row> rows = rowDataset.collectAsList();
        sparkSession.log().info("end"+flag);
        for (Row row : rows) {
            LinkedHashMap<String, Object> resultMap = new LinkedHashMap<>();
            int i = 0;
            for (String column : columns) {
                if(column.contains("[") && column.contains("]")){
                    String value = row.getAs(i).toString();
                    resultMap.put(column,value);
                }else{
                    resultMap.put(column,row.getAs(column));
                }
                i++;
            }
            results.add(resultMap);
        }
        if(results.size()==0 && ObjectUtils.isNotEmpty(columns) && columns.length!=0){
            LinkedHashMap<String, Object> resultMap = new LinkedHashMap<>();
            for (String column : columns) {
                resultMap.put(column,null);
            }
            results.add(resultMap);
        }
        return results;
    }

    public void deleteOldFiles(Table table) throws Exception{
        SparkActions.get(sparkSession).deleteOrphanFiles(table)
                .option("write.metadata.delete-after-commit.enabled","true")
                .option("write.metadata.previous-versions-max","1")
                .execute();
    }

    public void mergeFile(Table table) throws Exception{
        final SparkActions sparkActions = SparkActions.get(sparkSession);
        sparkActions.rewriteDataFiles(table).filter(Expressions.notNull("id"))
                .option("target-file-size-bytes",Long.toString(100 * 1024 * 1024)).execute();
        sparkActions.rewriteManifests(table).execute();
    }
}
