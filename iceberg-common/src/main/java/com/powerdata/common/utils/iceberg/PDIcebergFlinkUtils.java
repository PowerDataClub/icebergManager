package com.powerdata.common.utils.iceberg;

import org.apache.commons.lang3.ObjectUtils;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.ExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class PDIcebergFlinkUtils extends PDIcebergExecUtils {

    private static final Logger log = LoggerFactory.getLogger(PDIcebergFlinkUtils.class);

    private String catalogId;
    private String type;
    private String hiveUri;
    private String hdfsUri;
    private String databaseName;
    private String execUser;
    public StreamTableEnvironment tableEnvironment;
    public static HashMap<String, PDIcebergFlinkUtils> icebergFlinkClientsMap=new HashMap<>();
    public PDIcebergFlinkUtils(String catalogId, String type, String hiveUri, String hdfsUri,
                               String databaseName,String hadoopUser){
        this.execUser = hadoopUser;
        System.setProperty("HADOOP_USER_NAME", hadoopUser);
        this.catalogId = catalogId;
        this.type = type;
        this.hiveUri = hiveUri;
        this.hdfsUri = hdfsUri;
        this.databaseName = databaseName;
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
//        executionEnvironment.setParallelism(1);
        ExecutionConfig config = executionEnvironment.getConfig();
        config.setExecutionMode(ExecutionMode.BATCH);
//        executionEnvironment.enableCheckpointing(100);
        tableEnvironment = StreamTableEnvironment.create(executionEnvironment);
        String catalogSql = "create catalog ";
        if("hive".equals(type)){
            catalogSql = catalogSql + catalogId + " WITH (\n" +
                    "  'type'='iceberg',\n" +
                    "  'catalog-type'='hive',\n" +
                    "  'uri'='"+hiveUri+"',\n" +
                    "  'clients'='5',\n" +
                    "  'property-version'='1',\n" +
                    "  'warehouse'='"+hdfsUri+"'\n" +
                    ")";
        }else{
            catalogSql = catalogSql + catalogId + " WITH (\n" +
                    "  'type'='iceberg',\n" +
                    "  'catalog-type'='hadoop',\n" +
                    "  'warehouse'='"+hdfsUri+"',\n" +
                    "  'property-version'='1'\n" +
                    ")";
        }
        tableEnvironment.executeSql(catalogSql);
    }
    public static PDIcebergFlinkUtils build(String catalogId, String type, String hiveUri, String hdfsUri,
                                            String databaseName,String hadoopUser){
        return new PDIcebergFlinkUtils(catalogId,type,hiveUri,hdfsUri,databaseName,hadoopUser);
    }

    @Override
    public List<Map<String,Object>> executeSql(String sql){
        ArrayList<Map<String, Object>> results = new ArrayList<>();
        sql = sql.trim();
        if(sql.toLowerCase().startsWith("select") && !sql.contains("limit")){
            sql = sql +" limit 200 ";
        }
        TableResult tableResult=null;
        if(sql.toLowerCase().startsWith("select")
                || sql.toLowerCase().startsWith("set")){
            tableResult = tableEnvironment.sqlQuery(sql).execute();
        }else{
            tableResult = tableEnvironment.executeSql(sql);
        }
        List<String> columnNames = tableResult.getResolvedSchema().getColumnNames();
        CloseableIterator<Row> rowCloseableIterator = tableResult.collect();
        while (rowCloseableIterator.hasNext()) {
            LinkedHashMap<String, Object> data = new LinkedHashMap<>();
            Row rowData = rowCloseableIterator.next();
            for (int i = 0; i < columnNames.size(); i++) {
                data.put(columnNames.get(i),rowData.getField(i));
            }
            results.add(data);
        }
        return results;
    }

    @Override
    public List<Map<String,Object>> executeSql(String sql,String flag){
        new PDIcebergFlinkUtils(catalogId,type,hiveUri,hdfsUri,databaseName,execUser);
        ArrayList<Map<String, Object>> results = new ArrayList<>();
        sql = sql.trim();
        if(sql.toLowerCase().startsWith("select") && !sql.contains("limit")){
            sql = sql +" limit 200 ";
        }
        TableResult tableResult=null;
        log.info("begin"+flag);
        tableResult = tableEnvironment.executeSql(sql);
        log.info("end"+flag);
        List<String> columnNames = tableResult.getResolvedSchema().getColumnNames();
        CloseableIterator<Row> rowCloseableIterator = tableResult.collect();
        while (rowCloseableIterator.hasNext()) {
            LinkedHashMap<String, Object> data = new LinkedHashMap<>();
            Row rowData = rowCloseableIterator.next();
            for (int i = 0; i < columnNames.size(); i++) {
                data.put(columnNames.get(i),rowData.getField(i));
            }
            results.add(data);
        }
        if(results.size()==0 && ObjectUtils.isNotEmpty(columnNames) && columnNames.size()!=0){
            LinkedHashMap<String, Object> resultMap = new LinkedHashMap<>();
            for (String column : columnNames) {
                resultMap.put(column,null);
            }
            results.add(resultMap);
        }
        return results;
    }

}
