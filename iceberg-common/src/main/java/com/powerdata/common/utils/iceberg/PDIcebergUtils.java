package com.powerdata.common.utils.iceberg;

import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.powerdata.common.enums.IcebergBaseType;
import com.powerdata.common.utils.bean.table.TableColumnsBean;
import com.powerdata.common.utils.bean.table.TableMetricsBean;
import com.powerdata.common.utils.bean.table.TablePartitionKeysBean;
import com.powerdata.common.utils.bean.table.TableTransactionsBean;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.io.*;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import java.io.*;
import java.net.URI;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

public class PDIcebergUtils {

    private static final Logger log = LoggerFactory.getLogger(PDIcebergUtils.class);

//    private String uploadHiveFilePath = "/opt/icebergManager/hiveconf/";
    private BaseMetastoreCatalog catalog;
    private String type;
    private String warehouse;
    private String hiveUri;
    private String catalogId;

    private String execUser;

    private PDIcebergUtils(String catalogId, String type, String hiveUrl, String warehousePath,String hadoopUser,String uploadHiveFilePath) throws Exception {
        execUser = hadoopUser;
        System.setProperty("HADOOP_USER_NAME", hadoopUser);
        final Configuration configuration = new Configuration();
        if("hadoop".equals(type)){
            HadoopCatalog hadoopCatalog = new HadoopCatalog();
            hadoopCatalog.setConf(configuration);
            Map<String, String> prop = new HashMap<>();
            prop.put(CatalogProperties.WAREHOUSE_LOCATION,warehousePath);
            hadoopCatalog.initialize(catalogId,prop);
            catalog = hadoopCatalog;
        }else{
            String dir = uploadHiveFilePath+catalogId+"/";
            Set<String> fileList =
                    Arrays.stream(Objects.requireNonNull(new File(dir).listFiles()))
                            .map(File::getName)
                            .collect(Collectors.toSet());
            List<String> files = Arrays.asList("core-site.xml", "hdfs-site.xml", "hive-site.xml");
            if (!fileList.containsAll(files)) {
                throw new Exception("hive类型的Catalog缺失配置文件【core-site.xml、hdfs-site.xml、hive-site.xml】,请检查catalog管理是否维护配置文件");
            }

            HiveCatalog hiveCatalog = new HiveCatalog();
//            //TODO 本地测试调整为本地路径
//            configuration.addResource(new Path("F:\\core-site.xml"));
//            configuration.addResource(new Path("F:\\hdfs-site.xml"));
//            configuration.addResource(new Path("F:\\hive-site.xml"));
            configuration.addResource(new Path(dir+"core-site.xml"));
            configuration.addResource(new Path(dir+"hdfs-site.xml"));
            configuration.addResource(new Path(dir+"hive-site.xml"));
            hiveCatalog.setConf(configuration);
            Map<String, String> prop = new HashMap<>();
            prop.put(CatalogProperties.WAREHOUSE_LOCATION,warehousePath);
            prop.put(CatalogProperties.URI,hiveUrl);
            prop.put(CatalogProperties.CATALOG_IMPL,"org.apache.iceberg.hive.HiveCatalog");
            hiveCatalog.initialize(catalogId,prop);
            catalog = hiveCatalog;
        }
        this.warehouse = warehousePath;
        this.type = type;
        this.hiveUri = hiveUrl;
        this.catalogId = catalogId;
    }
    public static HashMap<String, PDIcebergUtils> hadoopCatalogHashMap=new HashMap<>();

    //初始化((HadoopCatalog)catalog)对象
    public static PDIcebergUtils build(String catalogId, String type, String hiveUrl, String warehousePath,String hadoopUser,String uploadHiveFilePath) throws Exception {
        PDIcebergUtils pdIcebergUtils = hadoopCatalogHashMap.get(catalogId);
        if(ObjectUtils.isEmpty(pdIcebergUtils)){
            hadoopCatalogHashMap.put(catalogId,new PDIcebergUtils(catalogId,type,hiveUrl,warehousePath,hadoopUser,uploadHiveFilePath));
        }
        return hadoopCatalogHashMap.get(catalogId);
    }

    //获取((HadoopCatalog)catalog)下的库名
    public List<String> listDataBases(){
        if("hive".equals(type)){
            List<Namespace> namespaces = ((HiveCatalog) catalog).listNamespaces();
            List<String> hiveDB = getHiveDB(warehouse);
            return namespaces.stream().map(Namespace::toString).filter(hiveDB::contains).collect(Collectors.toList());
        }
        return ((HadoopCatalog)catalog).listNamespaces().stream().map(Namespace::toString).collect(Collectors.toList());
    }
    //创建库
    public void addDatabases(String dataBaseName){
        if("hive".equals(type)){
            ((HiveCatalog)catalog).createNamespace(Namespace.of(dataBaseName));
        }else{
            ((HadoopCatalog)catalog).createNamespace(Namespace.of(dataBaseName));
        }
    }
    //删除库
    public void deleteDatabases(String dataBaseName){
        if("hive".equals(type)){
            ((HiveCatalog)catalog).dropNamespace(Namespace.of(dataBaseName));
        }else{
            ((HadoopCatalog)catalog).dropNamespace(Namespace.of(dataBaseName));
        }
    }

    //获取库下的表列表信息
    public List<String> listTable(String databaseName){
        if("hive".equals(type)){
            return ((HiveCatalog)catalog).listTables(Namespace.of(databaseName))
                    .stream()
                    .map(TableIdentifier::name)
                    .collect(Collectors.toList());
        }
        return ((HadoopCatalog)catalog).listTables(Namespace.of(databaseName))
                .stream()
                .map(TableIdentifier::name)
                .collect(Collectors.toList());
    }

    //获取表的元数据-metrics信息
    public TableMetricsBean getTableMetrics(String databaseName, String tableName) throws IOException {
        List<TableTransactionsBean> transactionsMessage = getTransactionsMessage(databaseName, tableName);
        if(ObjectUtils.isEmpty(transactionsMessage) || transactionsMessage.size()==0){
            return new TableMetricsBean();
        }
        Table table = getTable(TableIdentifier.of(databaseName, tableName));
        String dataPath = table.location() + "/data/";
        FileSystem fileSystem = FileSystem.get(URI.create(dataPath), new Configuration());
        HashMap<String, Double> tableSizeMap = new HashMap<>();
        tableSizeMap.put("tableSize",0.0);
        HashMap<String, Integer> filesMap = new HashMap<>();
        filesMap.put("files",0);

        if (!fileSystem.exists(new Path(dataPath))) {
            fileSystem.mkdirs(new Path(dataPath));
        }

        getMetrics(dataPath,fileSystem,tableSizeMap,filesMap);
        Double tableSize = tableSizeMap.get("tableSize");
        Integer files = filesMap.get("files");
        double avgFileSize = tableSize/(files==0?1:files);
        Long lastCommitTime = transactionsMessage.get(0).getCommitTime();
        Long records = getRecordsBySnapShotId(table, table.currentSnapshot().snapshotId());
        return new TableMetricsBean(formatDouble(tableSize/1000),files,formatDouble(avgFileSize/1000),
                lastCommitTime, records);
    }

    public Table getTable(TableIdentifier of) {
        Table table = null;
        if ("hive".equals(type)) {
            table = ((HiveCatalog) catalog).loadTable(of);
        } else {
            table = ((HadoopCatalog) catalog).loadTable(of);
        }
        return table;
    }
    public Boolean tableExists(String databaseName,String tableName){
        if ("hive".equals(type)) {
            return ((HiveCatalog) catalog).tableExists(TableIdentifier.of(databaseName,tableName));
        } else {
            return ((HadoopCatalog) catalog).tableExists(TableIdentifier.of(databaseName,tableName));
        }
    }

    //获取列信息
    public List<TableColumnsBean> getColumnsOfTable(String databaseName, String tableName){
        Table table = getTable(TableIdentifier.of(databaseName, tableName));
        return table.schema().columns().stream().map(column->
                new TableColumnsBean(column.fieldId(),column.name(),column.type().toString(),column.doc(),column.isOptional()))
                .sorted(Comparator.comparingInt(TableColumnsBean::getId))
                .collect(Collectors.toList());
    }


    //获取分区列表信息
    public List<TablePartitionKeysBean> getPartitionMessage(String databaseName, String tableName){
        Table table = getTable(TableIdentifier.of(databaseName, tableName));
        Schema schema = table.schema();
        return table.spec().fields().stream()
                .map(partitionField ->
                        new TablePartitionKeysBean(partitionField.fieldId(),
                                partitionField.name(),
                                schema.findColumnName(partitionField.sourceId()),
                                partitionField.transform().toString()))
                .sorted(Comparator.comparingInt(TablePartitionKeysBean::getId))
                .collect(Collectors.toList());
    }

    //获取执行事务列表
    public List<TableTransactionsBean> getTransactionsMessage(String databaseName, String tableName){
        ArrayList<TableTransactionsBean> resultList = new ArrayList<>();
        Table table = getTable(TableIdentifier.of(databaseName, tableName));
        Snapshot currentSnapshot1 = table.currentSnapshot();
        if(ObjectUtils.isEmpty(currentSnapshot1)){
            return resultList;
        }
        long currentSnapShotId = currentSnapshot1.snapshotId();
        for (Snapshot snapshot : table.snapshots()) {
            long transactionId = snapshot.snapshotId();
            String parentId = ObjectUtils.isEmpty(snapshot.parentId())?"":snapshot.parentId()+"";
            Map<String, String> summary = snapshot.summary();
            String operation = snapshot.operation();
            int addFiles = ObjectUtils.isEmpty(summary.get("added-data-files")) ? 0 :
                    new Integer(summary.get("added-data-files"));
            int deleteFiles = ObjectUtils.isEmpty(summary.get("deleted-data-files")) ? 0 :
                    new Integer(summary.get("deleted-data-files"));
            double addFileSize = ObjectUtils.isEmpty(summary.get("added-files-size")) ? 0 :
                    new Double(summary.get("added-files-size"));
            double deleteFIleSize = ObjectUtils.isEmpty(summary.get("removed-files-size")) ? 0 :
                    new Double(summary.get("removed-files-size"));
            long commit_time = snapshot.timestampMillis();
            long records = ObjectUtils.isEmpty(summary.get("total-records")) ? 0 :
                    new Long(summary.get("total-records"));
            TableTransactionsBean tableTransactionsBean =
                    new TableTransactionsBean(transactionId, operation, addFiles, deleteFiles,
                    formatDouble(addFileSize / 1024), formatDouble(deleteFIleSize / 1024),
                            commit_time, transactionId,parentId);
            if(new Long(tableTransactionsBean.getSnapshotId()) == currentSnapShotId){
                tableTransactionsBean.setIsCurrent(true);
            }
            tableTransactionsBean.setRecords(records);
            resultList.add(tableTransactionsBean);
        }
        resultList.sort((o1, o2) -> o2.getCommitTime().compareTo(o1.getCommitTime()));
        return resultList;
    }


    private Long getRecordsBySnapShotId(Table table, Long snapShotId){
        Snapshot snapshot = table.snapshot(snapShotId);
        if(ObjectUtils.isEmpty(snapshot)){
            return 0L;
        }
        Map<String, String> summary = snapshot.summary();
        return ObjectUtils.isEmpty(summary.get("total-records")) ? 0 :
                new Long(summary.get("total-records"));
    }

    //全量数据获取，接口返回不推荐直接调用，推荐分页处理
    public Map<String,Object> getTableData(String databaseName, String tableName, Integer pageSize, Integer pageNum) throws Exception {
        Table table = getTable(TableIdentifier.of(databaseName, tableName));
        List<String> colNames = getColumnNameList(table);
        CloseableIterable<Record> records =
                IcebergGenerics.read(table).build();
        Map<String, Object> result = getDataMapByPages(pageSize, pageNum, colNames, records);
        if(ObjectUtils.isEmpty(table.currentSnapshot())){
            result.put("total",0L);
        }else{
            result.put("total",getRecordsBySnapShotId(table,table.currentSnapshot().snapshotId()));
        }
        return result;
    }

    //指定snapshot，获取该snapshot下的数据状态
    public Map<String,Object> getTableData(String databaseName, String tableName,Long snapshotId,
                                                 Integer pageSize, Integer pageNum) throws Exception {
        Table table = getTable(TableIdentifier.of(databaseName, tableName));
        List<String> colNames = getColumnNameList(table);
        CloseableIterable<Record> records = IcebergGenerics.read(table).useSnapshot(snapshotId).build();
        Map<String, Object> result = getDataMapByPages(pageSize, pageNum, colNames, records);
        result.put("total",getRecordsBySnapShotId(table,table.snapshot(snapshotId).snapshotId()));
        return result;
    }

    //获取某个时间之前的数据状态
    public Map<String,Object> getTableDataBeforeTime(String databaseName, String tableName,Long asOfTime,
                                                           Integer pageSize, Integer pageNum) throws Exception {
        Table table = getTable(TableIdentifier.of(databaseName, tableName));
        List<String> colNames = getColumnNameList(table);
        String snapshotId = getTransactionsMessage(databaseName, tableName)
                .stream()
                .filter(a -> a.getCommitTime() <= asOfTime)
                .max(Comparator.comparing(TableTransactionsBean::getCommitTime))
                .get()
                .getSnapshotId();
        CloseableIterable<Record> records = IcebergGenerics.read(table).useSnapshot(new Long(snapshotId)).build();
        Map<String, Object> result = getDataMapByPages(pageSize, pageNum, colNames, records);
        result.put("total",getRecordsBySnapShotId(table,new Long(snapshotId)));
        return result;
    }

    //获取两个snapshot之间新增的数据//无总数
    public Map<String,Object> getAppendsTableData(String databaseName, String tableName,
                                                        Long fromSnapshotId,Long toSnapshotId,
                                                        Integer pageSize, Integer pageNum) throws Exception {
        Table table = getTable(TableIdentifier.of(databaseName, tableName));
        List<String> colNames = getColumnNameList(table);
        CloseableIterable<Record> records =
                IcebergGenerics.read(table).appendsBetween(fromSnapshotId, toSnapshotId).build();
        return getDataMapByPages(pageSize, pageNum, colNames, records);
    }

    //获取某个snapshot之后的新增数据
    public Map<String,Object> getAppendsTableData(String databaseName, String tableName,Long afterSnapshotId,
                                                        Integer pageSize, Integer pageNum) throws Exception {
        Table table = getTable(TableIdentifier.of(databaseName, tableName));
        List<String> colNames = getColumnNameList(table);
        CloseableIterable<Record> records = IcebergGenerics.read(table).appendsAfter(afterSnapshotId).build();
        return getDataMapByPages(pageSize, pageNum, colNames, records);
    }

    //新增数据（提供数据list和分隔符，数据导入可以使用该接口）
    public void addTableData(String databaseName,String tableName,List<String> addData,String splitCode) {
        Table table = getTable(TableIdentifier.of(databaseName, tableName));
        Schema schema = table.schema();
        List<String> colList =
                schema.columns()
                        .stream()
                        .sorted(Comparator.comparingInt(Types.NestedField::fieldId))
                        .map(a -> a.name()+","+a.type())
                        .collect(Collectors.toList());

        String filepath = table.location() + "/data/" + UUID.randomUUID() +".parquet";
        OutputFile file = table.io().newOutputFile(filepath);
        try {
            DataWriter<GenericRecord> dataWriter= Parquet.writeData(file)
                        .schema(schema)
                        .createWriterFunc(GenericParquetWriter::buildWriter)
                        .overwrite()
                        .withSpec(PartitionSpec.unpartitioned())
                        .build();
            for (String addDatum : addData) {
                String[] data = addDatum.split(splitCode);
                GenericRecord record = GenericRecord.create(schema);
                for (int i = 0; i < colList.size(); i++) {
                    String colValue = "";
                    if(i<data.length){
                        colValue = data[i];
                    }
                    String[] col = colList.get(i).split(",");
                    record.setField(col[0],getDataByType(col[1],colValue));
                }
                dataWriter.write(record);
            }
            dataWriter.close();
            DataFile dataFile = dataWriter.toDataFile();
            table.newAppend().appendFile(dataFile).commit();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void addTableDataOfJson(String databaseName, String tableName, ArrayList<String> addData) {
        Table table = getTable(TableIdentifier.of(databaseName, tableName));
        Schema schema = table.schema();
        List<String> colList =
                schema.columns()
                        .stream()
                        .sorted(Comparator.comparingInt(Types.NestedField::fieldId))
                        .map(a -> a.name()+","+a.type())
                        .collect(Collectors.toList());

        String filepath = table.location() + "/data/" + UUID.randomUUID() +".parquet";
        OutputFile file = table.io().newOutputFile(filepath);
        try {
            DataWriter<GenericRecord> dataWriter= Parquet.writeData(file)
                    .schema(schema)
                    .createWriterFunc(GenericParquetWriter::buildWriter)
                    .overwrite()
                    .withSpec(PartitionSpec.unpartitioned())
                    .build();
            for (String addDatum : addData) {
                if (!JSONUtil.isTypeJSON(addDatum)) {
                    continue;
                }
                JSONObject dataJson = JSONUtil.parseObj(addDatum);
                GenericRecord record = GenericRecord.create(schema);
                for (int i = 0; i < colList.size(); i++) {
                    String[] col = colList.get(i).split(",");
                    Object colValue = dataJson.getByPath(col[0]);
                    if(ObjectUtils.isEmpty(colValue)){
                        continue;
                    }
                    record.setField(col[0],getDataByType(col[1],colValue.toString()));
                }
                dataWriter.write(record);
            }
            dataWriter.close();
            DataFile dataFile = dataWriter.toDataFile();
            table.newAppend().appendFile(dataFile).commit();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void deleteHdfsFile(String filePath){
        String[] split = filePath.split(":8020");
        String hdfsUrl = split[0] + ":8020";
        String file = split[1];
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS",hdfsUrl);
        FileSystem fileSystem = null;
        try {
             fileSystem = FileSystem.get(configuration);
             if(fileSystem.isFile(new Path(file))){
                 fileSystem.delete(new Path(file),true);
             }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                Objects.requireNonNull(fileSystem).close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private List<String> getHiveDB(String filePath){
        ArrayList<String> result = new ArrayList<>();
        String[] split = filePath.split(":8020");
        String hdfsUrl = split[0] + ":8020";
        String file = split[1];
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS",hdfsUrl);
        FileSystem fileSystem = null;
        try {
            fileSystem = FileSystem.get(configuration);
            FileStatus[] fileStatuses = fileSystem.listStatus(new Path(file));
            for (FileStatus fileStatus : fileStatuses) {
                result.add(fileStatus.getPath().getName().split("\\.")[0]);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    public Object getDataByType(String types,String value){
        if(StringUtils.isEmpty(value)) {
            return null;
        }
        try{
            switch (types){
            case "boolean":
                return Boolean.valueOf(value);
            case "int":
                return new Integer(value);
            case "long":
                return new Long(value);
            case "double":
                return new Double(value);
            case "float":
                return new Float(value);
            case "binary":
                ByteBuffer byteBuffer = ByteBuffer.allocate(value.length());
                byteBuffer.put(value.getBytes());
                return byteBuffer;
            case "timestamptz":
                return LocalDateTime.parse(value).atOffset(ZoneOffset.UTC);
            default:
                return value;
            }
        }catch (Exception e){
            return null;
        }
    }

    //回滚snapshot
    public void rollbackSnapshot(String databaseName,String tableName,Long snapshotId){
        Table table = getTable(TableIdentifier.of(databaseName, tableName));
        table.manageSnapshots().rollbackTo(snapshotId).commit();
    }

    //设置当前的snapshot
    public void setCurrentSnapshot(String databaseName,String tableName,Long snapshotId){
        Table table = getTable(TableIdentifier.of(databaseName, tableName));
        table.manageSnapshots().setCurrentSnapshot(snapshotId).commit();
    }

    public void cherryPickSnapshot(String databaseName,String tableName,Long snapshotId){
        Table table = getTable(TableIdentifier.of(databaseName, tableName));
        table.manageSnapshots().cherrypick(snapshotId).commit();
    }

    public void mergeSmallFile(String databaseName,String tableName) throws Exception {
        PDIcebergSparkUtils sparkUtils = PDIcebergSparkUtils.build(catalogId, type, hiveUri, warehouse, databaseName,execUser);
        Table table = getTable(TableIdentifier.of(databaseName, tableName));
        String dataPath = table.location() + "/data/";
        String fileSql =
                "select file_path from `"+catalogId+"`.`"+databaseName+"`.`"+tableName+"`.files";
        String mergeFiles =
                "call `"+catalogId+"`.system.rewrite_data_files(table => '`"+databaseName+"`.`"
                        +tableName+"`',options => map('min-input-files','2'))";
        PDHdfsUtils hdfsUtils = PDHdfsUtils.build(catalogId, warehouse,execUser);
        List<String> oldFilePath =new ArrayList<>();
        hdfsUtils.getFiles(dataPath,oldFilePath);
        oldFilePath = oldFilePath.stream().map(files -> files.split("8020")[1]).collect(Collectors.toList());
        sparkUtils.executeSql(mergeFiles);
        List<String> newFilePath = sparkUtils.executeSql(fileSql).stream()
                .map(file -> {
                    Object file_path = file.get("file_path");
                    if(ObjectUtils.isNotEmpty(file_path)){
                        return file_path.toString().split("8020")[1];
                    }
                    return "";
                }).collect(Collectors.toList());
        oldFilePath.removeAll(newFilePath);
        for (String file : oldFilePath) {
            hdfsUtils.deleteFile(file);
        }
//        sparkUtils.mergeFile(table);
        expireOldSnapshots(table.currentSnapshot().timestampMillis()+1,table);
        deleteOldVersionFile(hdfsUtils,table);
    }

    public void expireOldSnapshots(Long tsToExpireTime,Table table) throws Exception{
        table.expireSnapshots().expireOlderThan(tsToExpireTime)
                .cleanExpiredFiles(true).retainLast(2).commit();
    }

    public boolean deleteOldVersionFile(PDHdfsUtils hdfsUtils, Table table) throws Exception {
        int snapshotSize = 0;
        for (Snapshot ignored : table.snapshots()) {
            snapshotSize++;
        }
        if(snapshotSize!=1){
            return false;
        }
        String metaPath = table.location() + "/metadata/";
        FileSystem fileSystem = hdfsUtils.getFileSystem();
        if("hadoop".equals(type)){
            String version = hdfsUtils.catFile(metaPath + "version-hint.text");
            List<String> deleteFiles = Arrays.stream(fileSystem.listStatus(new Path(metaPath)))
                    .map(fileStatus -> fileStatus.getPath().getName())
                    .filter(fileName -> fileName.endsWith(".json") &&
                            !("v"+version.replaceAll("\n","")+".metadata.json").equals(fileName))
                    .map(fileName->(metaPath+fileName).split("8020")[1])
                    .collect(Collectors.toList());
            for (String deleteFile : deleteFiles) {
                hdfsUtils.deleteFile(deleteFile);
            }
        }else{
            List<String> metaFiles = Arrays.stream(fileSystem.listStatus(new Path(metaPath)))
                    .map(fileStatus -> fileStatus.getPath().getName())
                    .filter(fileName -> fileName.endsWith(".json"))
                    .sorted(Comparator.naturalOrder())
                    .collect(Collectors.toList());
            for (int i = 0; i < metaFiles.size()-1; i++) {
                String deleteFilePath = (metaPath + metaFiles.get(i)).split("8020")[1];
                hdfsUtils.deleteFile(deleteFilePath);
            }
        }
        return true;
    }

    public void addTableDataWithoutPartition(String databaseName,String tableName,List<HashMap<String,String>> addData) {
        Table table = getTable(TableIdentifier.of(databaseName, tableName));
        Schema schema = table.schema();
        List<String> colList =
                schema.columns()
                        .stream()
                        .sorted(Comparator.comparingInt(Types.NestedField::fieldId))
                        .map(a -> a.name()+","+a.type())
                        .collect(Collectors.toList());
        GenericRecord record = GenericRecord.create(schema);
        String filepath = table.location() + "/data/" + UUID.randomUUID().toString()+".parquet";
        OutputFile file = table.io().newOutputFile(filepath);
        try {
            DataWriter<GenericRecord> dataWriter= Parquet.writeData(file)
                    .schema(schema)
                    .createWriterFunc(GenericParquetWriter::buildWriter)
                    .overwrite()
                    .withSpec(PartitionSpec.unpartitioned())
                    .build();
            for (HashMap<String, String> addDatum : addData) {
                for (String s : colList) {
                    String[] col = s.split(",");
                    if (ObjectUtils.isNotEmpty(addDatum.get(col[0]))) {
                        record.setField(col[0], getDataByType(col[1], addDatum.get(col[0])));
                    }
                }
                dataWriter.write(record);
            }
            dataWriter.close();
            DataFile dataFile = dataWriter.toDataFile();
            table.newAppend().appendFile(dataFile).commit();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Async
    public void addTableData(String databaseName,String tableName,List<HashMap<String,String>> addDataList) {
        Table table = getTable(TableIdentifier.of(databaseName, tableName));
        if (getPartitionMessage(databaseName,tableName).size() == 0 && addDataList.size()>100000) {
            addTableDataWithoutPartition(databaseName,tableName,addDataList);
        }else{
            Schema schema = table.schema();
            LinkedHashMap<String, String> colMap = new LinkedHashMap<>();
            for (String colNameAndTypes : schema.columns()
                    .stream()
                    .sorted(Comparator.comparingInt(Types.NestedField::fieldId))
                    .map(a -> a.name() + "|" + a.type().toString())
                    .collect(Collectors.toList())) {
                String[] split = colNameAndTypes.split("\\|");
                colMap.put(split[0],split[1]);
            }

            StringBuilder colNameStr= new StringBuilder();
            for (Map.Entry<String, String> col : colMap.entrySet()) {
                colNameStr.append(col.getKey()).append(",");
            }
            colNameStr = new StringBuilder(colNameStr.substring(0, colNameStr.length() - 1));
            StringBuilder colValue = new StringBuilder();
            PDIcebergSparkUtils pdIcebergSparkUtils =
                PDIcebergSparkUtils.build(catalogId,type, hiveUri, warehouse,databaseName,execUser);
            String insertTable = databaseName+"."+tableName;
            insertTable = catalogId+"."+insertTable;
            for (HashMap<String, String> addDatum : addDataList) {
                StringBuilder midColValue = new StringBuilder("(");
                for (Map.Entry<String, String> col : colMap.entrySet()) {
                    midColValue.append(getDataByType(addDatum, col.getKey(), col.getValue())).append(",");
                }
                midColValue = new StringBuilder(midColValue.substring(0, midColValue.length() - 1) + ")");
                colValue.append(midColValue).append(",");

                if(colValue.toString().getBytes().length > 50*1024*1024){
                    String addSql =
                            "insert into "+insertTable+"("+colNameStr+") values "+colValue.substring(0,colValue.length()-1);
                    pdIcebergSparkUtils.executeSql(addSql);
                    colValue = new StringBuilder();
                }
            }
            if(colValue.length()>0){
                String addSql =
                        "insert into "+insertTable+"("+colNameStr+") values "+colValue.substring(0,colValue.length()-1);
                pdIcebergSparkUtils.executeSql(addSql);
            }

        }
    }

    public void deleteTableData(String databaseName,String tableName,List<HashMap<String,String>> addDataList){
        Table table = getTable(TableIdentifier.of(databaseName, tableName));
        Schema schema = table.schema();
        LinkedHashMap<String, String> colMap = new LinkedHashMap<>();
        for (String colNameAndTypes : schema.columns().stream().sorted(new Comparator<Types.NestedField>() {
            @Override
            public int compare(Types.NestedField o1, Types.NestedField o2) {
                return o1.fieldId() - o2.fieldId();
            }
        }).map(a -> a.name() + "|" + a.type().toString()).collect(Collectors.toList())) {
            String[] split = colNameAndTypes.split("\\|");
            colMap.put(split[0],split[1]);
        }

        PDIcebergSparkUtils pdIcebergSparkUtils =
                PDIcebergSparkUtils.build(catalogId,type, hiveUri, warehouse,databaseName,execUser);
        String deleteTable = "`"+databaseName+"`.`"+tableName+"`";
        deleteTable = "`"+catalogId+"`."+deleteTable+"";

        for (HashMap<String, String> addDatum : addDataList) {
            String colValue = " 1=1 ";
            StringBuilder midColValue = new StringBuilder();
            for (Map.Entry<String, String> col : colMap.entrySet()) {
                midColValue.append(" and `").append(col.getKey()).append("`=").append(getDataByType(addDatum, col.getKey(), col.getValue()));
            }
            colValue = colValue + midColValue;
            int times = countSql(pdIcebergSparkUtils, deleteTable,colValue);
            String addSql = "delete from "+deleteTable+" where "+colValue;
            log.info("deleteTableData:"+addSql);
            pdIcebergSparkUtils.executeSql(addSql);
            if(times>1){
                insertSqls(databaseName,tableName,times-1,addDatum);
            }
        }
    }

    private int countSql(PDIcebergSparkUtils pdIcebergSparkUtils, String tableName, String whereSql){
        String countSql = "select count(*) as counts from "+tableName + " where "+ whereSql;
        log.info("countSql:"+countSql);
        List<Map<String, Object>> maps = pdIcebergSparkUtils.executeSql(countSql);
        return new Integer(maps.get(0).get("counts").toString());
    }

    private void insertSqls(String databaseName,
                            String tableName,int times, HashMap<String,String> addDatum){
        ArrayList<HashMap<String, String>> addDataList = new ArrayList<>();
        for (int i=0;i<times;i++){
            addDataList.add(addDatum);
        }
        addTableData(databaseName,tableName,addDataList);
    }

    private String getDataByType(HashMap<String, String> addDatum, String colName, String type) {
        String value = addDatum.get(colName);
        if(StringUtils.isEmpty(value)){
            return "''";
        }
        List<String> numberTypes = Arrays.asList("int", "float", "double", "long");
        if(numberTypes.contains(type.toLowerCase())
                || type.toLowerCase().contains("map")
                || type.toLowerCase().contains("list")){
            return value;
        }
        return "'"+value+"'";
    }

    private Map<String,Object> getDataMapByPages(Integer pageSize, Integer pageNum,
                                                 List<String> colNames,
                                                 CloseableIterable<Record> records) throws Exception {
        if(ObjectUtils.isEmpty(records)){
            throw new Exception("数据读取失败");
        }
        pageNum=1;
        HashMap<String, Object> resultMap = new HashMap<>();
        ArrayList<Map<String,Object>> resultList = new ArrayList<>();
        int i = 0;
        int skip = (pageNum - 1) * pageSize;
        for (Record record : records) {
            if (i >= skip + pageSize) {
                break;
            }
            Map<String, Object> recordMap = new LinkedHashMap<>();
            for (String colName : colNames) {
                recordMap.put(colName, record.getField(colName));
            }
            resultList.add(recordMap);
            i++;
        }
        resultMap.put("list", resultList);
        return resultMap;
    }

    @NotNull
    private List<String> getColumnNameList(Table table) {
        return table.schema().columns()
                .stream()
                .sorted(Comparator.comparingInt(Types.NestedField::fieldId))
                .map(Types.NestedField::name)
                .collect(Collectors.toList());
    }

    private Double formatDouble(Double num){
        if(ObjectUtils.isEmpty(num) || num==0d){
            return 0d;
        }
        DecimalFormat df = new DecimalFormat("#.##");
        return new Double(df.format(num));
    }

    public void deleteTable(String databaseName, String tableName){
        if("hive".equals(type)){
            ((HiveCatalog)catalog).dropTable(TableIdentifier.of(databaseName,tableName));
        }else{
            ((HadoopCatalog)catalog).dropTable(TableIdentifier.of(databaseName,tableName));
        }
    }

    public void renameTable(String databaseName, String tableName, String newDatabaseName, String newTableName){
        if("hive".equals(type)){
            ((HiveCatalog)catalog).renameTable(TableIdentifier.of(databaseName,tableName),
                    TableIdentifier.of(newDatabaseName,newTableName));
        }else{
            ((HadoopCatalog)catalog).renameTable(TableIdentifier.of(databaseName,tableName),
                    TableIdentifier.of(newDatabaseName,newTableName));
        }
    }

    public void getMetrics(String dataPath,FileSystem fileSystem,Map<String,Double> tableSizeMap,Map<String,Integer> filesMap)
            throws IOException {
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path(dataPath));
        for (FileStatus fileStatus : fileStatuses) {
            if (fileStatus.isDirectory()) {
                getMetrics(dataPath + fileStatus.getPath().getName() + "/", fileSystem, tableSizeMap, filesMap);
            } else {
                tableSizeMap.put("tableSize", tableSizeMap.get("tableSize") + fileStatus.getLen());
                filesMap.put("files", filesMap.get("files") + 1);
            }
        }
    }

    public Map<String,Object> getMetadataFiles(String databaseName,String tableName,String fileName,
                                               Integer pageSize,Integer pageNum) throws IOException {
        Table table = getTable(TableIdentifier.of(databaseName, tableName));
        String metadataPath = table.location() + "/metadata/";
        FileSystem fileSystem = FileSystem.get(URI.create(metadataPath), new Configuration());
        return getMetadataFiles(metadataPath,fileSystem,fileName,pageSize,pageNum);
    }


    public String getMetadata(String databaseName, String tableName, String fileName) throws Exception{
        Table table = getTable(TableIdentifier.of(databaseName, tableName));
        String metadataPath = table.location() + "/metadata/"+fileName;

        if(fileName.endsWith(".avro")){
            return PDHdfsUtils.catAvroFile(metadataPath);
        }

        StringBuilder result = new StringBuilder();
        String readLine = "";
        FileSystem fileSystem = FileSystem.get(URI.create(metadataPath), new Configuration());
        Path path = new Path(metadataPath);
        FSDataInputStream open = fileSystem.open(path);

        while((readLine = open.readLine()) != null){
            result.append(readLine).append("\n");
        }
        open.close();
        return result.toString();
    }

    private Map<String,Object> getMetadataFiles(String metaPath,FileSystem fileSystem,String fileName,Integer pageSize,Integer pageNum)
            throws IOException{
        HashMap<String, Object> result = new HashMap<>();
        List<String> files =
                Arrays.stream(fileSystem.listStatus(new Path(metaPath)))
                        .filter(FileStatus::isFile)
                        .map(a -> a.getPath().getName())
                        .filter(a-> a.contains(fileName) || StringUtils.isEmpty(fileName))
                        .sorted(Comparator.naturalOrder())
                        .collect(Collectors.toList());
        result.put("total",files.size());
        List<String> viewFiles =
                files.stream()
                        .skip((long) (pageNum - 1) * pageSize)
                        .limit(pageSize)
                        .collect(Collectors.toList());
        result.put("data",viewFiles);
        return result;
    }

    public void createTable(String databaseName, String tableName, List<Map<String, String>> createData,
                            List<Map<String, String>> partitionParam) {
        // 校验分区字段中的字段名是不是表字段名
        List<String> columnNameList = createData.stream()
                .map(stringStringMap -> stringStringMap.get("columnName")).collect(Collectors.toList());
        List<String> partitionNameList = partitionParam.stream()
                .map(stringStringMap -> stringStringMap.get("sourceName")).collect(Collectors.toList());

        if (!new HashSet<>(columnNameList).containsAll(partitionNameList)) {
            throw new RuntimeException("分区字段必须是表字段！");
        }

        List<Types.NestedField> list = new ArrayList<>();
        int index = 1;
        for (Map<String, String> entry : createData) {
            if (MapUtils.isEmpty(entry)) {
                continue;
            }
            String columnName = entry.get("columnName");
            String dataType = entry.get("dataType");
            // 1 不为空必填是false  有点绕
            boolean isNullable = !StringUtils.equals(entry.get("isNullable"), "1");
            String comment = entry.get("comment");
            IcebergBaseType icebergBaseType = IcebergBaseType.getType(dataType);
            if (icebergBaseType != null) {
                list.add(Types.NestedField.of(index++, isNullable, columnName, icebergBaseType.getType(), comment));
                continue;
            }
            switch (dataType) {
                case "Decimal":
                    String precision = entry.get("precision");
                    String scale = entry.get("scale");
                    if (StringUtils.isBlank(precision) || StringUtils.isBlank(scale)) {
                        throw new RuntimeException("Decimal类型需要总位数和小数位！");
                    }
                    list.add(Types.NestedField.of(index++, isNullable, columnName,
                            Types.DecimalType.of(Integer.parseInt(precision), Integer.parseInt(scale)), comment));
                    break;
                case "Map":
                    String keyType = entry.get("keyType");
                    IcebergBaseType keyTypeEnum = IcebergBaseType.getType(keyType);
                    if (keyTypeEnum == null) {
                        throw new RuntimeException("map字段key未定义类型！");
                    }
                    String valueType = entry.get("valueType");
                    IcebergBaseType valueTypeEnum = IcebergBaseType.getType(valueType);
                    if (valueTypeEnum == null) {
                        throw new RuntimeException("map字段value未定义类型！");
                    }
                    int cid = index;
                    int keyId = ++index;
                    int valueId = ++index;
                    if (StringUtils.equals(entry.get("isNullableSubType"), "1")) {
                        list.add(Types.NestedField.of(cid, isNullable, columnName,
                                Types.MapType.ofRequired(keyId, valueId, keyTypeEnum.getType(),
                                        valueTypeEnum.getType()), comment));
                    } else {
                        list.add(Types.NestedField.of(cid, isNullable, columnName,
                                Types.MapType.ofOptional(keyId, valueId, keyTypeEnum.getType(),
                                        valueTypeEnum.getType()), comment));
                    }
                    index++;

                    break;
                case "Timestamp":
                    if (StringUtils.equals(entry.get("isWithZone"), "1")) {
                        list.add(Types.NestedField.of(index++, isNullable, columnName,
                                Types.TimestampType.withZone(), comment));
                    } else {
                        list.add(Types.NestedField.of(index++, isNullable, columnName,
                                Types.TimestampType.withoutZone(), comment));
                    }
                    break;
                case "List":
                    String listValueType = entry.get("valueType");
                    IcebergBaseType listValueTypeEnum = IcebergBaseType.getType(listValueType);
                    if (listValueTypeEnum == null) {
                        throw new RuntimeException("list字段元素未定义类型！");
                    }
                    int c_id = index;
                    int kId = ++index;
                    if (StringUtils.equals(entry.get("isNullableSubType"), "1")) {
                        list.add(Types.NestedField.of(c_id, isNullable, columnName,
                                Types.ListType.ofRequired(kId, listValueTypeEnum.getType()), comment));

                    } else {
                        list.add(Types.NestedField.of(c_id, isNullable, columnName,
                                Types.ListType.ofOptional(kId, listValueTypeEnum.getType()), comment));
                    }
                    index++;
                    break;
                default:
                    break;
            }
        }
        Schema schema = new Schema(list);

        PartitionSpec.Builder builder = PartitionSpec.builderFor(schema);

        for (Map<String, String> partitionEntry : partitionParam) {
            String sourceName = partitionEntry.get("sourceName");
            String type = partitionEntry.get("type");
            if (StringUtils.isBlank(type) || StringUtils.equals(type,"identity")  ) {

                builder = builder.identity(sourceName);

            } else {
                String targetName = partitionEntry.get("targetName");
                switch (type) {
                    case "hour":
                        builder = StringUtils.isNotBlank(targetName) ?
                                builder.hour(sourceName, targetName) : builder.hour(sourceName);
                        break;
                    case "day":
                        builder = StringUtils.isNotBlank(targetName) ?
                                builder.day(sourceName, targetName) : builder.hour(sourceName);
                        break;
                    case "month":
                        builder = StringUtils.isNotBlank(targetName) ?
                                builder.month(sourceName, targetName) : builder.hour(sourceName);
                        break;
                    case "year":
                        builder = StringUtils.isNotBlank(targetName) ?
                                builder.year(sourceName, targetName) : builder.hour(sourceName);
                        break;
                }
            }
        }

        PartitionSpec partitionSpec = builder.build();

        try {

            TableIdentifier tableIdentifier = TableIdentifier.of(databaseName, tableName);
            if("hive".equals(type)){
                ((HiveCatalog)catalog).createTable(tableIdentifier, schema, partitionSpec);
            }else{
                ((HadoopCatalog)catalog).createTable(tableIdentifier, schema, partitionSpec);
            }
        } catch (Exception e) {

            throw new RuntimeException(e);

        }
    }

    public void getCreateTableSql(String databaseName,String tableName) throws Exception{
        Table table = getTable(TableIdentifier.of(databaseName, tableName));
        System.out.println(table.schema().toString());
        System.out.println(table.spec());
    }


    public void updateTable(String databaseName, String tableName,List<Map<String, String>> createData,
                            List<Map<String, String>> partitionParam) {
        List<TableTransactionsBean> transactionsMessage = getTransactionsMessage(databaseName, tableName);
        if (CollectionUtils.isEmpty(transactionsMessage)){
            // 先创建一个临时表,临时表创建成功证明，可以创建成功
            createTable(databaseName, tableName+"_tmp", createData, partitionParam);
            deleteTable(databaseName, tableName);
            createTable(databaseName, tableName, createData, partitionParam);
            deleteTable(databaseName, tableName+"_tmp");
            return;
        }


        TableIdentifier tableIdentifier = TableIdentifier.of(databaseName, tableName);
        Table table = getTable(tableIdentifier);
        UpdateSchema updateSchema = table.updateSchema();

        String lastColumn = createData.get(0).get("columnName");
        for (Map<String, String> entry : createData) {
            String curColumnName = entry.get("columnName");
            String comment = entry.get("comment");

            switch (entry.get("exectype")) {

                case "0"://不调整
                    lastColumn = curColumnName;
                    break;
                case "1"://新增字段
                    String dataType = entry.get("dataType");
                    IcebergBaseType icebergBaseType = IcebergBaseType.getType(dataType);
                    Type type = null;
                    if (null == icebergBaseType) {
                        switch (dataType) {
                            case "Decimal":
                                String precision = entry.get("precision");
                                String scale = entry.get("scale");
                                if (StringUtils.isBlank(precision) || StringUtils.isBlank(scale)) {
                                    throw new RuntimeException("Decimal类型需要总位数和小数位！");
                                }
                                type = Types.DecimalType.of(Integer.parseInt(precision), Integer.parseInt(scale));
                                break;
                            case "Map":
                                String keyType = entry.get("keyType");
                                IcebergBaseType keyTypeEnum = IcebergBaseType.getType(keyType);
                                if (keyTypeEnum == null) {
                                    throw new RuntimeException("map字段key未定义类型！");
                                }
                                String valueType = entry.get("valueType");
                                IcebergBaseType valueTypeEnum = IcebergBaseType.getType(valueType);
                                if (valueTypeEnum == null) {
                                    throw new RuntimeException("map字段value未定义类型！");
                                }
                                type = Types.MapType.ofOptional(1, 2, keyTypeEnum.getType(), valueTypeEnum.getType());
                                break;
                            case "Timestamp":
                                if (StringUtils.equals(entry.get("isWithZone"), "1")) {
                                    type = Types.TimestampType.withZone();
                                } else {
                                    type = Types.TimestampType.withoutZone();
                                }
                                break;
                            case "List":
                                String listValueType = entry.get("valueType");
                                IcebergBaseType listValueTypeEnum = IcebergBaseType.getType(listValueType);
                                if (listValueTypeEnum == null) {
                                    throw new RuntimeException("list字段元素未定义类型！");
                                }
                                type = Types.ListType.ofOptional(1, listValueTypeEnum.getType());
                                break;
                            default:
                                break;
                        }
                    } else {
                        type = icebergBaseType.getType();
                    }
                    // 1=必填
                    updateSchema = StringUtils.equals(entry.get("isNullable"), "1") ?
                            updateSchema.addRequiredColumn(curColumnName, type, comment) :
                            updateSchema.addColumn(curColumnName, type, comment);
                    break;
                case "2"://修改字段
                    String newColumnName = entry.get("newColumnName");
                    String newComment = entry.get("newComment");
                    String newIsNullable = entry.get("newIsNullable");
                    if (StringUtils.isNotBlank(newColumnName)) {
                        updateSchema = updateSchema.renameColumn(curColumnName, newColumnName);
                    }
                    String newDataType = entry.get("newDataType");
                    if (StringUtils.isNotBlank(newDataType)) {
                        IcebergBaseType newType = IcebergBaseType.getType(newDataType);
                        if (null != newType)
                            updateSchema = updateSchema.updateColumn(curColumnName, newType.getType().asPrimitiveType());
                    }
                    if (StringUtils.isNotBlank(newComment)) {
                        updateSchema = updateSchema.updateColumnDoc(curColumnName, newComment);
                    }
                    if (StringUtils.isNotBlank(newIsNullable) && StringUtils.equals("0",newIsNullable)) {
                        updateSchema = updateSchema.makeColumnOptional(curColumnName);
                    }

                    break;
                case "3"://删除字段
                    updateSchema.deleteColumn(curColumnName);
                    break;
            }
        }
        updateSchema.commit();
        for (Map<String, String> partitionEntry : partitionParam) {
            String exectype = partitionEntry.get("exectype");
            if(!"1".equals(exectype)){
                continue;
            }
            String sourceName = partitionEntry.get("sourceName");
            String types = partitionEntry.get("type");
            String targetName = partitionEntry.get("targetName");
            String addPartitions = "alter table "+catalogId+"."+databaseName+"."+tableName+" add partition field ";
            if("identity".equals(types)){
                addPartitions = addPartitions +sourceName;
            }else{
                addPartitions = addPartitions + types+"s("+sourceName+")";
            }
            PDIcebergSparkUtils.build(catalogId,type,hiveUri,warehouse,databaseName,execUser).executeSql(addPartitions);
        }

    }

    public Boolean isExitTable(String databaseName,String tableName) {
        return catalog.tableExists(TableIdentifier.of(databaseName, tableName));
    }

    public ArrayList<String> getAllTableData(String databaseName, String tableName, String splitStr,String isHead) throws Exception {
        ArrayList<String> resultList = new ArrayList<>();
        Table table = getTable(TableIdentifier.of(databaseName, tableName));
        List<String> colNames = getColumnNameList(table);
        if("1".equals(isHead)){
            StringBuilder midStr= new StringBuilder();
            int i=1;
            for (String colName : colNames) {
                if(i==colNames.size()){
                    midStr.append(colName);
                }else{
                    midStr.append(colName).append(splitStr);
                }
                i++;
            }
            resultList.add(midStr.toString());
        }
        CloseableIterable<Record> records = IcebergGenerics.read(table).build();
        if(ObjectUtils.isEmpty(records)){
            throw new Exception("表数据为空");
        }
        for (Record record : records) {
            StringBuilder midStr= new StringBuilder();
            int i=1;
            for (String colName : colNames) {
                Object fieldValue = record.getField(colName);
                String value="";
                if(ObjectUtils.isNotEmpty(fieldValue)){
                    value = fieldValue.toString();
                }
                if(i==colNames.size()){
                    midStr.append(value.replaceAll("\n" , " "));
                }else{
                    midStr.append(value.replaceAll("\n" , " ")).append(splitStr);
                }
                i++;
            }
            resultList.add(midStr.toString());
        }
        return resultList;
    }

    public void tableDataToFileWithBranch(String databaseName, String tableName, FSDataOutputStream fileOutPutStream,
                                          String splitStr, String isHead) throws Exception {
        Table table = getTable(TableIdentifier.of(databaseName, tableName));
        List<String> colNames = getColumnNameList(table);
        BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(fileOutPutStream));
        try{
            if("1".equals(isHead)){
                StringBuilder midStr= new StringBuilder();
                int i=1;
                for (String colName : colNames) {
                    if(i==colNames.size()){
                        midStr.append(colName);
                    }else{
                        midStr.append(colName).append(splitStr);
                    }
                    i++;
                }
                bufferedWriter.write(midStr.toString());
                bufferedWriter.newLine();
            }
            CloseableIterable<Record> records = IcebergGenerics.read(table).build();
            if(ObjectUtils.isEmpty(records)){
                throw new Exception("表数据为空");
            }
            for (Record record : records) {
                StringBuilder midStr= new StringBuilder();
                int i=1;
                for (String colName : colNames) {
                    Object fieldValue = record.getField(colName);
                    String value="";
                    if(ObjectUtils.isNotEmpty(fieldValue)){
                        value = fieldValue.toString();
                    }
                    if(i==colNames.size()){
                        midStr.append(value.replaceAll("\n" , " "));
                    }else{
                        midStr.append(value.replaceAll("\n" , " ")).append(splitStr);
                    }
                    i++;
                }
                bufferedWriter.write(midStr.toString());
                bufferedWriter.newLine();
            }
        }catch (Exception e){
            throw new Exception(e);
        }finally {
            bufferedWriter.close();
            fileOutPutStream.close();
        }

    }

    public void updateData(String databaseName, String tableName,
                           HashMap<String, String> oldData, HashMap<String, String> newData) throws Exception{
        ArrayList<HashMap<String, String>> oldDataList = new ArrayList<>();
        ArrayList<HashMap<String, String>> newDataList = new ArrayList<>();
        oldDataList.add(oldData);
        newDataList.add(newData);
        deleteTableData(databaseName,tableName,oldDataList);
        addTableData(databaseName,tableName,newDataList);
    }

    public int addTableDataFromHDFSFile(String databaseName, String tableName, InputStreamReader inputStreamReader,
                                         String splitStr, String isHead,List<String> addStr) throws Exception {
        int count = 0;
        BufferedReader bufferedReader = new BufferedReader(inputStreamReader);

        Table table = getTable(TableIdentifier.of(databaseName, tableName));
        Schema schema = table.schema();
        List<String> colList =
                schema.columns()
                        .stream()
                        .sorted(Comparator.comparingInt(Types.NestedField::fieldId))
                        .map(a -> a.name()+","+a.type())
                        .collect(Collectors.toList());
        GenericRecord record = GenericRecord.create(schema);
        String filepath = table.location() + "/data/" + UUID.randomUUID() +".parquet";
        OutputFile file = table.io().newOutputFile(filepath);
        try {
            DataWriter<GenericRecord> dataWriter= Parquet.writeData(file)
                    .schema(schema)
                    .createWriterFunc(GenericParquetWriter::buildWriter)
                    .overwrite()
                    .withSpec(PartitionSpec.unpartitioned())
                    .build();
            String readLine = "";
            while((readLine = bufferedReader.readLine()) != null){
                if (readLine.endsWith(splitStr)){
                    readLine = readLine+" ";
                }
                String[] lineData = readLine.split(splitStr);
                int checkLength = lineData.length;
                if(addStr!=null){
                    checkLength += addStr.size();
                }
                if(checkLength != colList.size()){
                    throw new Exception("数据列数和表字段数不匹配");
                }
                if("1".equals(isHead)){
                    isHead="0";
                    continue;
                }
                for (int i = 0; i < colList.size(); i++) {
                    String[] col = colList.get(i).split(",");
                    if(i<lineData.length-1){
                        if(ObjectUtils.isNotEmpty(lineData[i])){
                            record.setField(col[0],getDataByType(col[1],lineData[i]));
                        }
                    } else if (i==lineData.length-1) {
                        if(!readLine.endsWith(splitStr)){
                            if(ObjectUtils.isNotEmpty(lineData[i])){
                                record.setField(col[0],getDataByType(col[1],lineData[i]));
                            }
                        }
                    } else {
                        if(ObjectUtils.isNotEmpty(Objects.requireNonNull(addStr).get(i-lineData.length))){
                            record.setField(col[0],getDataByType(col[1],addStr.get(i-lineData.length)));
                        }
                    }
                }
                dataWriter.write(record);
                count++;
            }
            dataWriter.close();
            DataFile dataFile = dataWriter.toDataFile();
            table.newAppend().appendFile(dataFile).commit();
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            bufferedReader.close();
            inputStreamReader.close();
        }
        return count;
    }

    public int syncHiveToIcebergTable(PDHdfsUtils pdHdfsUtils, String tablePath, String splitStr,
                                      String distdatabase, String disttable) throws Exception {
        int count = 0;
        ArrayList<String> filePaths = new ArrayList<>();
        FileSystem fileSystem = pdHdfsUtils.getFileSystem();
        pdHdfsUtils.getFiles(tablePath,filePaths);
        for (String filePath : filePaths) {
            InputStreamReader inputStreamReader = null;
            FSDataInputStream fsDataInputStream = fileSystem.open(new Path(filePath));
            String partitionStr = filePath.split(tablePath + "/")[1];
            List<String> addList = new ArrayList<>();
            if(partitionStr.contains("/")){
                String[] partitions = partitionStr.split("/");
                for (int i = 0; i < partitions.length-1; i++) {
                    String[] partition = partitions[i].split("=");
                    addList.add(partition[1]);
                }
            }
            if(filePath.endsWith(".gz")){
                inputStreamReader = new InputStreamReader(new GZIPInputStream(fsDataInputStream));
            }else{
                inputStreamReader = new InputStreamReader(fsDataInputStream);
            }
            count += addTableDataFromHDFSFile(distdatabase, disttable, inputStreamReader, splitStr, "0", addList);
        }
        return count;
    }

    public int syncHDFSToIcebergTable(PDHdfsUtils pdHdfsUtils, String syncdata, String splitStr,
                                      String distdatabase, String disttable) throws Exception {
        FileSystem fileSystem = pdHdfsUtils.getFileSystem();
        FSDataInputStream fsDataInputStream = fileSystem.open(new Path(syncdata));
        return addTableDataFromHDFSFile(distdatabase,disttable,new InputStreamReader(fsDataInputStream),splitStr,"0",null);
    }

    public int syncMysqlToIcebergTable(Connection connection, String mysqlTable, String databaseName,
                                   String tableName) throws Exception {
        int count = 0;
        Statement statement = connection.createStatement();
        String mysqlDataSql = "select * from "+mysqlTable;
        ResultSet resultSet = statement.executeQuery(mysqlDataSql);
        try {
            Table table = getTable(TableIdentifier.of(databaseName, tableName));
            Schema schema = table.schema();
            List<String> colList = schema.columns().stream().sorted(new Comparator<org.apache.iceberg.types.Types.NestedField>() {
                @Override
                public int compare(org.apache.iceberg.types.Types.NestedField o1, Types.NestedField o2) {
                    return o1.fieldId() - o2.fieldId();
                }
            }).map(a -> a.name()+","+a.type()).collect(Collectors.toList());
            GenericRecord record = GenericRecord.create(schema);
            String filepath = table.location() + "/data/" + UUID.randomUUID().toString()+".parquet";
            OutputFile file = table.io().newOutputFile(filepath);
            DataWriter<GenericRecord> dataWriter= Parquet.writeData(file)
                    .schema(schema)
                    .createWriterFunc(GenericParquetWriter::buildWriter)
                    .overwrite()
                    .withSpec(PartitionSpec.unpartitioned())
                    .build();

            while(resultSet.next()){
                for (int i = 0; i < colList.size(); i++) {
                    String[] col = colList.get(i).split(",");
                    record.setField(col[0],getDataByType(col[1],resultSet.getString(i+1)));
                }
                dataWriter.write(record);
                count++;
            }
            dataWriter.close();
            DataFile dataFile = dataWriter.toDataFile();
            table.newAppend().appendFile(dataFile).commit();
        } catch (Exception e){
            e.printStackTrace();
            throw new Exception(e.getMessage());
        }finally {
            if(resultSet != null){
                resultSet.close();
            }
            statement.close();
        }
        return count;
    }
}
