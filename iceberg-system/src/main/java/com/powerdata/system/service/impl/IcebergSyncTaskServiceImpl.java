package com.powerdata.system.service.impl;

import cn.hutool.core.date.DateUtil;
import com.powerdata.common.utils.SecurityUtils;
import com.powerdata.common.utils.StringUtils;
import com.powerdata.common.utils.bean.BeanUtils;
import com.powerdata.common.utils.iceberg.PDHdfsUtils;
import com.powerdata.common.utils.iceberg.PDHiveUtils;
import com.powerdata.common.utils.iceberg.PDIcebergSparkUtils;
import com.powerdata.common.utils.iceberg.PDIcebergUtils;
import com.powerdata.system.domain.*;
import com.powerdata.system.domain.param.IcebergSyncTaskParam;
import com.powerdata.system.mapper.IcebergCatalogMapper;
import com.powerdata.system.mapper.IcebergSyncTaskMapper;
import com.powerdata.system.service.IcebergSyncTaskService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.ObjectUtils;

import javax.annotation.Resource;
import java.sql.*;
import java.time.Duration;
import java.util.*;
import java.util.Date;
import java.util.stream.Collectors;

/**
 * @author dinghao
 * @version 1.0
 * @description
 * @date 2023/8/29 16:09
 */
@Service
public class IcebergSyncTaskServiceImpl implements IcebergSyncTaskService {
    static Map<String,String> mysqlToIcebergMap=new HashMap<>();
    static{
        mysqlToIcebergMap.put("tinyint","int");
        mysqlToIcebergMap.put("smallint","int");
        mysqlToIcebergMap.put("mediumint","int");
        mysqlToIcebergMap.put("int","int");
        mysqlToIcebergMap.put("bigint","long");
        mysqlToIcebergMap.put("float","float");
        mysqlToIcebergMap.put("double","double");
        mysqlToIcebergMap.put("decimal","decimal");
        mysqlToIcebergMap.put("date","date");
        mysqlToIcebergMap.put("time","string");
        mysqlToIcebergMap.put("datetime","timestamp");
        mysqlToIcebergMap.put("char","string");
        mysqlToIcebergMap.put("varchar","string");
        mysqlToIcebergMap.put("binary","bytes");
        mysqlToIcebergMap.put("varbinary","bytes");
        mysqlToIcebergMap.put("longbolb","bytes");
        mysqlToIcebergMap.put("json","string");
    }


    private static final Logger logger = LoggerFactory.getLogger(IcebergSyncTaskServiceImpl.class);

    @Value(value = "${icebergManager.hiveConf}")
    private String uploadHiveFilePath;
    @Value(value = "${icebergManager.hadoopUser}")
    private String hadoopUser;
    @Value(value = "${icebergManager.hiveUser}")
    private String hiveUser;

    @Value(value = "${icebergManager.nameservices}")
    private String nameServer;

    @Resource
    private IcebergSyncTaskMapper icebergSyncTaskMapper;

    @Resource
    private IcebergCatalogMapper icebergCatalogMapper;

    @Override
    public Map<String, Object> taskList(IcebergSyncTaskParam icebergSyncTaskParam) throws Exception {
        final HashMap<String, Object> result = new HashMap<>();
        int pageSize = ObjectUtils.isEmpty(icebergSyncTaskParam.getPageSize()) ? 10:icebergSyncTaskParam.getPageSize();
        int pageNum = ObjectUtils.isEmpty(icebergSyncTaskParam.getPageNum()) ? 1:icebergSyncTaskParam.getPageNum();
        String orderByColumn =
                StringUtils.isEmpty(icebergSyncTaskParam.getOrderByColumn()) ? "id":icebergSyncTaskParam.getOrderByColumn();
        String isAsc = StringUtils.isEmpty(icebergSyncTaskParam.getIsAsc()) ? "desc":icebergSyncTaskParam.getIsAsc();

        IcebergSyncTaskExample icebergSyncTaskExample = new IcebergSyncTaskExample();
        IcebergSyncTaskExample.Criteria criteria = icebergSyncTaskExample.createCriteria();

        if(StringUtils.isNotEmpty(icebergSyncTaskParam.getSourcetypes())){
            criteria.andSourcetypesEqualTo(icebergSyncTaskParam.getSourcetypes());
        }

        if(StringUtils.isNotEmpty(icebergSyncTaskParam.getSourceip())){
            criteria.andSourceipLike("%"+icebergSyncTaskParam.getSourceip()+"%");
        }

        if(StringUtils.isNotEmpty(icebergSyncTaskParam.getSyncdata())){
            criteria.andSyncdataLike("%"+icebergSyncTaskParam.getSyncdata()+"%");
        }

        if(StringUtils.isNotEmpty(icebergSyncTaskParam.getDistcatalogid())){
            criteria.andDistcatalogidLike("%"+icebergSyncTaskParam.getDistcatalogid()+"%");
        }

        if(StringUtils.isNotEmpty(icebergSyncTaskParam.getDistdatabase())){
            criteria.andDistdatabaseLike("%"+icebergSyncTaskParam.getDistdatabase()+"%");
        }

        if(StringUtils.isNotEmpty(icebergSyncTaskParam.getDisttable())){
            criteria.andDisttableLike("%"+icebergSyncTaskParam.getDisttable()+"%");
        }

        if(StringUtils.isNotEmpty(icebergSyncTaskParam.getStatus())){
            criteria.andStatusEqualTo(icebergSyncTaskParam.getStatus());
        }

        if(StringUtils.isNotEmpty(icebergSyncTaskParam.getCreater())){
            criteria.andCreaterLike("%"+icebergSyncTaskParam.getCreater()+"%");
        }

        List<IcebergSyncTask> icebergSyncTaskList = icebergSyncTaskMapper.selectByExample(icebergSyncTaskExample);

        if(ObjectUtils.isEmpty(icebergSyncTaskList)||icebergSyncTaskList.size()==0){
            result.put("total",0);
            result.put("list",null);
            return result;
        }

        result.put("total",icebergSyncTaskList.size());
        icebergSyncTaskExample.setOrderByClause(" "+orderByColumn+" "+ isAsc+" limit "+ (pageNum-1)*pageSize+","+pageSize);
        List<IcebergSyncTask> resultList = icebergSyncTaskMapper.selectByExample(icebergSyncTaskExample)
                .stream().peek(task -> {
                    if("0".equals(task.getStatus())){
                        task.setExectimes(DateUtil.formatBetween(new Date(new Long(task.getCreatetime())), new Date()));
                    }
                }).collect(Collectors.toList());
        result.put("list", resultList);
        return result;
    }

    @Override
    public void add(IcebergSyncTask icebergSyncTask) throws Exception {
        String id = icebergSyncTask.getId();
        if(StringUtils.isNotEmpty(id)){
            IcebergSyncTask icebergSyncTask1 = icebergSyncTaskMapper.selectByPrimaryKey(id);
            if("0".equals(icebergSyncTask1.getStatus())){
                throw new Exception("运行中任务不支持再次执行，请重新创建任务");
            }
        }
        icebergSyncTask.setCreater(SecurityUtils.getLoginUser().getUsername());
        icebergSyncTask.setCreatetime(new Date().getTime()+"");
        icebergSyncTask.setId(icebergSyncTask.getCreatetime());
        icebergSyncTask.setStatus("0");
        icebergSyncTask.setOther1("");
        icebergSyncTask.setExectimes("");
        icebergSyncTaskMapper.insert(icebergSyncTask);
        Thread syncTaskThread = new Thread(() -> {
            Date begin = new Date();
            try {
                execSyncTask(icebergSyncTask);
            } catch (Exception e) {
                icebergSyncTask.setStatus("1");
                icebergSyncTask.setOther1(e.getMessage().substring(0, Math.min(e.getMessage().length(), 2048)));
                icebergSyncTask.setExectimes(DateUtil.formatBetween(begin, new Date()));
                icebergSyncTaskMapper.updateByPrimaryKey(icebergSyncTask);
                e.printStackTrace();
            }
        });
        syncTaskThread.setPriority(1);
        syncTaskThread.setName(icebergSyncTask.getSourcetypes()+"_"+icebergSyncTask.getId());
        syncTaskThread.start();
    }

    @Override
    public void stopKafkaTask(IcebergSyncTask icebergSyncTask) throws Exception {
        IcebergSyncTask icebergSyncTask1 = icebergSyncTaskMapper.selectByPrimaryKey(icebergSyncTask.getId());
        Date begin = new Date(Long.parseLong(icebergSyncTask1.getCreatetime()));
        icebergSyncTask1.setStatus("3");
        icebergSyncTask1.setExectimes(DateUtil.formatBetween(begin,new Date()));
        icebergSyncTaskMapper.updateByPrimaryKey(icebergSyncTask1);
    }


    private void execSyncTask(IcebergSyncTask icebergSyncTask) throws Exception {
        int count = 0;
        String distCatalogId = icebergSyncTask.getDistcatalogid();
        IcebergCatalog icebergCatalog = icebergCatalogMapper.selectByPrimaryKey(distCatalogId);
         IcebergSyncTaskParam icebergSyncTaskParam =
                new IcebergSyncTaskParam(icebergCatalog.getTypes(), icebergCatalog.getHiveurl(),
                        icebergCatalog.getHdfsurl(), icebergSyncTask);
        Date begin = new Date();
        //TODO 入湖任务执行逻辑
        switch (icebergSyncTask.getSourcetypes()){
            case "1":
                count = execSyncTaskOfHdfs(icebergSyncTaskParam);
                break;
            case "2":
                count = execSyncTaskOfHive(icebergSyncTaskParam);
                break;
            case "3":
                count = execSyncTaskOfKafka(icebergSyncTaskParam);
                break;
            case "4":
                count = execSyncTaskOfMysql(icebergSyncTaskParam);
                break;
            default:
                count = 0;
                break;
        }
        IcebergSyncTask icebergSyncTask1 = icebergSyncTaskMapper.selectByPrimaryKey(icebergSyncTask.getId());
        icebergSyncTask1.setExectimes(DateUtil.formatBetween(begin,new Date()));
        icebergSyncTask1.setOther1("总接入数据条数："+count);
        if(!"3".equals(icebergSyncTask1.getSourcetypes())){
            icebergSyncTask1.setStatus("2");
        }
        icebergSyncTaskMapper.updateByPrimaryKey(icebergSyncTask1);
    }

    private int execSyncTaskOfMysql(IcebergSyncTaskParam icebergSyncTaskParam) throws Exception {
        String syncdata = icebergSyncTaskParam.getSyncdata();
        String ip = icebergSyncTaskParam.getSourceip();
        String port = icebergSyncTaskParam.getOther2();
        String userName = icebergSyncTaskParam.getOther3();
        String password = icebergSyncTaskParam.getOther4();
        String catalogId = icebergSyncTaskParam.getDistcatalogid();
        String databaseName = icebergSyncTaskParam.getDistdatabase();
        String tableName = icebergSyncTaskParam.getDisttable();
        String types = icebergSyncTaskParam.getTypes();
        String hdfsUrl = icebergSyncTaskParam.getHdfsUrl();
        String hiveUrl = icebergSyncTaskParam.getHiveUrl();

        String url = "jdbc:mysql://"+ip+":"+port;
        Class.forName("com.mysql.cj.jdbc.Driver");
        Connection connection = null;
        String connectCheck="Too many connections";
        while("Too many connections".equals(connectCheck)){
            try{
                connection = DriverManager.getConnection(url, userName, password);
                connectCheck = "OK";
            }catch (Exception e){
                Thread.sleep(5000);
                if(e.getMessage().contains("Too many connections")){
                    connectCheck = "Too many connections";
                }else{
                    connectCheck = e.getMessage();
                }
            }
        }
        try{
            PDIcebergUtils pdIcebergUtils = PDIcebergUtils.build(catalogId, types, hiveUrl, hdfsUrl,hadoopUser,uploadHiveFilePath);
            if (!pdIcebergUtils.tableExists(databaseName, tableName)) {
                String createTableSqlStr =
                        mysqlToIcebergSqlOfCreateTableSql(Objects.requireNonNull(connection), syncdata, catalogId, databaseName, tableName);
                logger.info("execSyncTaskOfMysql_createTableSql:"+createTableSqlStr);
                PDIcebergSparkUtils.build(catalogId,types,hiveUrl,hdfsUrl,null,hadoopUser)
                        .executeSql(createTableSqlStr);
            }
            return pdIcebergUtils.syncMysqlToIcebergTable(Objects.requireNonNull(connection), syncdata, databaseName, tableName);
        } catch (Exception e){
            e.printStackTrace();
            throw new Exception(e.getMessage());
        }finally {
            Objects.requireNonNull(connection).close();
        }
    }

    @Override
    public void batchAddHiveTask(IcebergSyncTask icebergSyncTask) throws Exception {
        String sourceIp = icebergSyncTask.getSourceip();
        String syncdata = icebergSyncTask.getSyncdata();
        List<String> allTableNames = PDHiveUtils.build(sourceIp,hiveUser).getAllTableNames(syncdata);
        for (String tableName : allTableNames) {
            IcebergSyncTask icebergSyncTask1 = new IcebergSyncTask();
            BeanUtils.copyBeanProp(icebergSyncTask1,icebergSyncTask);
            icebergSyncTask1.setSyncdata(icebergSyncTask.getSyncdata().split("\\.")[0]+"."+tableName);
            icebergSyncTask1.setDisttable(tableName);
            icebergSyncTask1.setId("");
            add(icebergSyncTask1);
        }
    }

    @Override
    public void batchAddMySqlTask(IcebergSyncTask icebergSyncTask) throws Exception {
        String syncdata = icebergSyncTask.getSyncdata();
        String ip = icebergSyncTask.getSourceip();
        String port = icebergSyncTask.getOther2();
        String userName = icebergSyncTask.getOther3();
        String password = icebergSyncTask.getOther4();
        String url = "jdbc:mysql://"+ip+":"+port;
        Class.forName("com.mysql.cj.jdbc.Driver");
        Connection connection = DriverManager.getConnection(url, userName, password);
        Statement statement = connection.createStatement();
        try{
            statement.execute("use "+syncdata);
            ResultSet resultSet = statement.executeQuery("show tables");
            while(resultSet.next()) {
                String tableName = resultSet.getString(1);
                IcebergSyncTask icebergSyncTask1 = new IcebergSyncTask();
                BeanUtils.copyBeanProp(icebergSyncTask1,icebergSyncTask);
                icebergSyncTask1.setSyncdata(icebergSyncTask.getSyncdata().split("\\.")[0]+"."+tableName);
                icebergSyncTask1.setDisttable(tableName);
                icebergSyncTask1.setId("");
                add(icebergSyncTask1);
            }
            resultSet.close();
        }catch (Exception e){
            e.printStackTrace();
            throw new Exception(e.getMessage());
        }finally {
            statement.close();
            connection.close();
        }
    }

    private int execSyncTaskOfKafka(IcebergSyncTaskParam icebergSyncTaskParam) throws Exception {
        String id = icebergSyncTaskParam.getId();
        String kafkaIp = icebergSyncTaskParam.getSourceip();
        String groupId = icebergSyncTaskParam.toString();
        String topicName = icebergSyncTaskParam.getSyncdata();

        String catalogId = icebergSyncTaskParam.getDistcatalogid();
        String types = icebergSyncTaskParam.getTypes();
        String hdfsUrl = icebergSyncTaskParam.getHdfsUrl();
        String hiveUrl = icebergSyncTaskParam.getHiveUrl();
        String distDB = icebergSyncTaskParam.getDistdatabase();
        String distTBL = icebergSyncTaskParam.getDisttable();
        String splitStr = icebergSyncTaskParam.getOther2();
        String dataType = icebergSyncTaskParam.getOther1();

        PDIcebergUtils pdIcebergUtils = PDIcebergUtils.build(catalogId, types, hiveUrl, hdfsUrl,hadoopUser,uploadHiveFilePath);

        Properties properties = new Properties();
        properties.put("bootstrap.servers", kafkaIp + ":9092");
        properties.put("group.id", groupId);
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("auto.offset.reset", "earliest");
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
        kafkaConsumer.subscribe(Collections.singletonList(topicName));
        int count = 0;
        while (!"3".equals(icebergSyncTaskMapper.selectByPrimaryKey(id).getStatus())) {
            logger.info("kafka入湖任务["+id+"]执行中："+new Date()+"");
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(5000));
            count+=records.count();
            logger.info("kafka入湖任务["+id+"]执行中："+records.count());
            ArrayList<String> recordArr = new ArrayList<>();
            for (ConsumerRecord<String, String> record : records) {
                recordArr.add(record.value());
            }
            if(recordArr.size()>0){
                if("json".equals(dataType)){
                    pdIcebergUtils.addTableDataOfJson(distDB, distTBL, recordArr);
                } else {
                    pdIcebergUtils.addTableData(distDB, distTBL, recordArr, splitStr);
                }
            }
            if(records.count()>0){
                IcebergSyncTask icebergSyncTask = icebergSyncTaskMapper.selectByPrimaryKey(id);
                icebergSyncTask.setOther1("接入数据条数："+count);
                icebergSyncTaskMapper.updateByPrimaryKey(icebergSyncTask);
            }
        }
        kafkaConsumer.close(Duration.ofMillis(1000));
        return count;
    }

    private int execSyncTaskOfHdfs(IcebergSyncTaskParam icebergSyncTaskParam) throws Exception {
        String sourceIP = icebergSyncTaskParam.getSourceip();
        String syncdata = icebergSyncTaskParam.getSyncdata();
        String catalogId = icebergSyncTaskParam.getDistcatalogid();
        String types = icebergSyncTaskParam.getTypes();
        String hdfsUrl = icebergSyncTaskParam.getHdfsUrl();
        String hiveUrl = icebergSyncTaskParam.getHiveUrl();
        String distDB = icebergSyncTaskParam.getDistdatabase();
        String distTBL = icebergSyncTaskParam.getDisttable();
        String splitStr = icebergSyncTaskParam.getOther2();
        PDHdfsUtils pdHdfsUtils =
                PDHdfsUtils.build(sourceIP, "hdfs://" + sourceIP + ":8020",hadoopUser);
        return PDIcebergUtils.build(catalogId, types, hiveUrl, hdfsUrl,hadoopUser,uploadHiveFilePath)
                .syncHDFSToIcebergTable(pdHdfsUtils, syncdata, splitStr, distDB, distTBL);
    }

    private int execSyncTaskOfHive(IcebergSyncTaskParam icebergSyncTaskParam) throws Exception {
        String sourceIP = icebergSyncTaskParam.getSourceip();
        String syncdata = icebergSyncTaskParam.getSyncdata();
        String nodeIP = icebergSyncTaskParam.getOther2();
        Map<String, String> tableMessage = PDHiveUtils.build(sourceIP,hiveUser).getTableMessage(syncdata);
        String tablePath =
                tableMessage.get("filePath").replace(nameServer,nodeIP+":8020").split("8020")[1];
        String splitStr = tableMessage.get("splitStr");
        String catalogId = icebergSyncTaskParam.getDistcatalogid();
        String types = icebergSyncTaskParam.getTypes();
        String hdfsUrl = icebergSyncTaskParam.getHdfsUrl();
        String hiveUrl = icebergSyncTaskParam.getHiveUrl();
        String distDB = icebergSyncTaskParam.getDistdatabase();
        String distTBL = icebergSyncTaskParam.getDisttable();
        String createSql = tableMessage.get("createSql").replace(syncdata,distTBL);
        PDHdfsUtils pdHdfsUtils =
                PDHdfsUtils.build(sourceIP, "hdfs://" + nodeIP + ":8020",hadoopUser);
        PDIcebergSparkUtils pdIcebergSparkUtils =
                PDIcebergSparkUtils.build(catalogId, types, hiveUrl, hdfsUrl, null,hadoopUser);
        pdIcebergSparkUtils.executeSql("use `" + catalogId + "`");
        pdIcebergSparkUtils.executeSql("use `"+distDB+"`");
        boolean isFlag = true;
        for (Map<String, Object> tables : pdIcebergSparkUtils.executeSql("show tables")) {
            if(distTBL.equals(tables.get("tableName"))){
                isFlag = false;
                break;
            }
        }
        if(isFlag){
            pdIcebergSparkUtils.executeSql(createSql);
        }
        return PDIcebergUtils.build(catalogId, types, hiveUrl, hdfsUrl,hadoopUser,uploadHiveFilePath)
                .syncHiveToIcebergTable(pdHdfsUtils, tablePath, splitStr, distDB, distTBL);
    }

    private String mysqlToIcebergSqlOfCreateTableSql(Connection connection,String mysqlTable,
                                                  String catalogId,String databaseName,String tableName) throws Exception {
        StringBuilder icebergCreateTableSql = new StringBuilder("create table " + catalogId + "." + databaseName + "." + tableName + " (");
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("show create table "+mysqlTable);
        String sql = "";
        while (resultSet.next()) {
            sql = resultSet.getString("Create Table");
        }
        resultSet.close();
        statement.close();

        List<String> sqlList = Arrays.asList(sql.toLowerCase().split("\n"));
        List<String> newCols = sqlList.stream().map(String::trim).filter(col -> col.startsWith("`")).map(col -> {
            String newCol = "";
            String[] comments = col.split(" comment ");
            String[] colMessageArr = comments[0].trim().split(" ");
            String[] types = colMessageArr[1].split("\\(");
            String colType = mysqlToIcebergMap.get(types[0]);
            if(colType == null){
                colType = "string";
            }
            newCol = colMessageArr[0] + " " + colType;
            if (comments.length == 2) {
                if (!comments[1].trim().endsWith(",")) {
                    comments[1] = comments[1]+",";
                }
                newCol = newCol + " comment " + comments[1];
            }else{
                newCol = newCol + ",";
            }
            return newCol;
        }).collect(Collectors.toList());
        for (String newCol : newCols) {
            icebergCreateTableSql.append(newCol);
        }
        return icebergCreateTableSql.substring(0,icebergCreateTableSql.length()-1)+" ) using iceberg";
    }

}
