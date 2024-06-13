package com.powerdata.system.service.impl;

import cn.hutool.core.date.DateUtil;
import com.powerdata.common.utils.SecurityUtils;
import com.powerdata.common.utils.StringUtils;
import com.powerdata.common.utils.exec.PDJschUtils;
import com.powerdata.common.utils.iceberg.*;
import com.powerdata.system.domain.IcebergExecLog;
import com.powerdata.system.domain.param.IcebergCatalogParam;
import com.powerdata.system.domain.param.IcebergCopyTableParam;
import com.powerdata.system.domain.param.IcebergTableParam;
import com.powerdata.system.mapper.IcebergExecLogMapper;
import com.powerdata.system.service.ISqlExecuteService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.ObjectUtils;

import javax.annotation.Resource;
import java.io.*;
import java.util.*;

/**
 * @author dinghao
 * @version 1.0
 * @description
 * @date 2023/6/16 15:04
 */
@Service
public class SqlExecuteServiceImpl implements ISqlExecuteService {

    private static final Logger log = LoggerFactory.getLogger(SqlExecuteServiceImpl.class);

    //现在获取日志的方式很呆，且问题很大，后面会调整
    public static String sqlLogPath = "./logs/sys-info.log";

    @Value(value = "${icebergManager.hiveConf}")
    private String uploadHiveFilePath;
    @Value(value = "${icebergManager.hadoopUser}")
    private String hadoopUser;

    @Value(value = "${icebergManager.sparkHome}")
    private String sparkHome;
    @Value(value = "${icebergManager.sparkExecHost}")
    private String sparkExecHost;
    @Value(value = "${icebergManager.sparkExecPort}")
    private String sparkExecPort;
    @Value(value = "${icebergManager.sparkExecUser}")
    private String sparkExecUser;
    @Value(value = "${icebergManager.sparkExecPassWd}")
    private String sparkExecPassWd;
    @Resource
    private IcebergExecLogMapper icebergExecLogMapper;

    @Override
    public Map<String, Object> executeSql(IcebergCatalogParam icebergCatalogParam) throws Exception {
        LinkedHashMap<String, Object> resultMap = new LinkedHashMap<>();
        String execType = icebergCatalogParam.getExecType();
        String mode = icebergCatalogParam.getMode();
        PDIcebergExecUtils PDIcebergExecUtils = null;
        IcebergExecLog icebergExecLog = new IcebergExecLog();

        if(!"local".equals(mode) && "spark".equals(execType)){
            Date begin = new Date();
            icebergExecLog.setId(begin.getTime()+"");
            icebergExecLog.setOther2(icebergCatalogParam.getId()+","+icebergCatalogParam.getDatabaseName()+","+execType+","+mode);
            icebergExecLog.setCreater(SecurityUtils.getLoginUser().getUsername());
            icebergExecLog.setStatus("0");
            icebergExecLog.setCreatetime(begin.getTime()+"");
            icebergExecLog.setExectimes("-");
            icebergExecLog.setSqlstr(icebergCatalogParam.getExecSql());
            icebergExecLog.setOther1("-");
            icebergExecLogMapper.insertSelective(icebergExecLog);

            if(icebergCatalogParam.getExecSql().contains(";")){
                throw new Exception("sparkOnYarn执行机不支持执行多条sql命令");
            }
            PDJschUtils execOnYarn = null;
            HashMap<String, Object> dataMap = new HashMap<>();
            try{
                execOnYarn = PDJschUtils.build(sparkExecHost, Integer.parseInt(sparkExecPort), sparkExecUser, sparkExecPassWd);
                String execSql = initExecSql(icebergCatalogParam);
                Map<String, String> execResult = execOnYarn.exec(execSql);
                String runLog = execResult.get("runLog");
                String errLog = execResult.get("errLog");
                if("0".equals(execResult.get("resultKey"))){
                    String[] response_codes = runLog.split("Response code");
                    String[] split = errLog.split("Time taken:");
                    String responseData= response_codes[1];
                    if(StringUtils.isEmpty(responseData)){
                        dataMap.put("data","sql执行成功");
                    }else{
                        List<Map<String, Object>> data = responseToData(responseData);
                        if(ObjectUtils.isEmpty(data) || data.size() == 0){
                            dataMap.put("data","sql执行成功");
                        }else{
                            dataMap.put("data",data);
                        }
                    }
                    dataMap.put("execTime",split[split.length-1]);
                    dataMap.put("log",errLog);
                    icebergExecLog.setStatus("2");
                    icebergExecLog.setOther1(errLog+"\ndata:"+runLog);
                } else {
                    dataMap.put("data",errLog);
                    dataMap.put("log",errLog);
                    icebergExecLog.setStatus("1");
                    icebergExecLog.setOther1(errLog);
                }
            } catch (Exception e){
                e.printStackTrace();
                dataMap.put("log",e.getMessage());
                icebergExecLog.setStatus("1");
                icebergExecLog.setOther1(e.getStackTrace().toString());
                resultMap.put(icebergCatalogParam.getExecSql(),dataMap);
                icebergExecLog.setExectimes(DateUtil.formatBetween(begin, new Date()));
                icebergExecLogMapper.updateByPrimaryKeySelective(icebergExecLog);
                throw new Exception(e.getMessage());
            } finally {
                Objects.requireNonNull(execOnYarn).close();
            }
            resultMap.put(icebergCatalogParam.getExecSql(),dataMap);

            icebergExecLog.setExectimes(DateUtil.formatBetween(begin, new Date()));
            icebergExecLogMapper.updateByPrimaryKeySelective(icebergExecLog);

            return resultMap;
        }

        if("spark".equals(execType)){
            PDIcebergExecUtils =
                    PDIcebergSparkUtils.build(icebergCatalogParam.getId(),icebergCatalogParam.getTypes(),
                            icebergCatalogParam.getHiveurl(), icebergCatalogParam.getHdfsurl(),
                            icebergCatalogParam.getDatabaseName(),hadoopUser);
        }else{
            PDIcebergExecUtils =
                    PDIcebergFlinkUtils.build(icebergCatalogParam.getId(), icebergCatalogParam.getTypes(),
                            icebergCatalogParam.getHiveurl(), icebergCatalogParam.getHdfsurl(),
                            icebergCatalogParam.getDatabaseName(),hadoopUser);
        }
        icebergExecLog.setOther2(icebergCatalogParam.getId()+","+icebergCatalogParam.getDatabaseName()+","+execType+","+mode);
        String sqlStr = icebergCatalogParam.getExecSql().replaceAll("\n", " ").trim();
        while(sqlStr.contains("  ")||sqlStr.contains(";;")){
            sqlStr = sqlStr.replaceAll("  "," ").replaceAll(";;",";");
        }
        String[] sqlArr = sqlStr.split(";");
        if("spark".equals(execType)) {
            PDIcebergExecUtils.executeSql("use `" + icebergCatalogParam.getId() + "`");
        }else{
            PDIcebergExecUtils.executeSql("use catalog `"+icebergCatalogParam.getId()+"`");
        }
        PDIcebergExecUtils.executeSql("use `"+icebergCatalogParam.getDatabaseName()+"`");
        int i = 0;
        for (String sql : sqlArr) {
            i++;
            HashMap<String, Object> dataMap = new HashMap<>();
            String execSql = sql.trim();
            if(execSql.startsWith("#")){
                continue;
            }
            Date begin = new Date();
            String flag = begin.getTime()+"";

            icebergExecLog.setId(flag);
            icebergExecLog.setCreater(SecurityUtils.getLoginUser().getUsername());
            icebergExecLog.setStatus("0");
            icebergExecLog.setCreatetime(flag);
            icebergExecLog.setExectimes("-");
            icebergExecLog.setSqlstr(sql);
            icebergExecLog.setOther1("-");
            icebergExecLogMapper.insertSelective(icebergExecLog);

            log.info("执行sql："+execSql);

            try{
                List<Map<String, Object>> sqlData = PDIcebergExecUtils.executeSql(execSql,flag);
                dataMap.put("execTime", DateUtil.formatBetween(begin,new Date()));
                dataMap.put("data",sqlData);
                if(ObjectUtils.isEmpty(sqlData) || sqlData.size() == 0){
                    dataMap.put("data","sql执行成功");
                }
                icebergExecLog.setStatus("2");
            }catch (Exception e){
                dataMap.put("execTime", DateUtil.formatBetween(begin,new Date()));
                dataMap.put("data","sql执行失败："+e.getMessage());
                icebergExecLog.setStatus("1");
                icebergExecLog.setOther1(e.getMessage());
            }
            String log = getLog(flag);
            dataMap.put("log",log);
            resultMap.put("["+i+"]"+execSql,dataMap);
            if("2".equals(icebergExecLog.getStatus())){
                icebergExecLog.setOther1(log.substring(0,Math.min(log.length(),4096)));
            }

            icebergExecLog.setExectimes(dataMap.get("execTime").toString());
            icebergExecLogMapper.updateByPrimaryKeySelective(icebergExecLog);
        }
        return resultMap;
    }

    private List<Map<String, Object>> responseToData(String responseData) {
        ArrayList<Map<String, Object>> results = new ArrayList<>();

        String[] responseDataArr = responseData.split("\n");
        String[] colNameArr = responseDataArr[1].split("\t");
        for (int i = 2; i < responseDataArr.length; i++) {
            LinkedHashMap<String, Object> resultMap = new LinkedHashMap<>();
            String[] dataArr = responseDataArr[i].split("\t");
            if(dataArr.length!=colNameArr.length){
                continue;
            }
            for (int j = 0; j < dataArr.length; j++) {
                resultMap.put(colNameArr[j],dataArr[j]);
            }
            results.add(resultMap);
        }
        return results;
    }

    private String initExecSql(IcebergCatalogParam icebergCatalogParam) {
        String types = icebergCatalogParam.getTypes();
        StringBuilder execSql = new StringBuilder("");
        String sql = "use "+icebergCatalogParam.getId()+"."+icebergCatalogParam.getDatabaseName()+";"
                +icebergCatalogParam.getExecSql();
        execSql.append(sparkHome+"/bin/spark-sql ")
                .append("--master yarn ")
                .append("--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions ");
        if("hive".equals(types)){
            execSql.append("--conf spark.sql.catalog.").append(icebergCatalogParam.getId()).append("=org.apache.iceberg.spark.SparkSessionCatalog ")
                    .append("--conf spark.sql.catalog.").append(icebergCatalogParam.getId()).append(".type=hive ");
        } else {
            execSql.append("--conf spark.sql.catalog.").append(icebergCatalogParam.getId()).append("=org.apache.iceberg.spark.SparkCatalog ")
                    .append("--conf spark.sql.catalog.").append(icebergCatalogParam.getId()).append(".type=hadoop ")
                    .append("--conf spark.sql.catalog.").append(icebergCatalogParam.getId()).append(".warehouse=")
                    .append(icebergCatalogParam.getHdfsurl()).append(" ");
        }
        execSql.append("--hiveconf hive.cli.print.header=true ")
                .append("-e \"").append(sql).append("\"");
        return execSql.toString();
    }

    private String getLog(String flag){
        String resultStr="";
        File file = new File(sqlLogPath);
        BufferedReader br = null;
        FileReader fr = null;
        try {
            fr = new FileReader(file);
            br = new BufferedReader(fr);
            String line;
            int i = 0;
            boolean begin = false;
            while ((line = br.readLine()) != null) {
                if(begin || line.contains("begin"+flag)){
                    begin = true;
                }
                if(!begin){
                    continue;
                }
                if(i==1000 || line.contains("end"+flag)){
                    break;
                }
                i++;
                resultStr = resultStr+line+"\n";
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                Objects.requireNonNull(br).close();
                Objects.requireNonNull(fr).close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return resultStr;
    }

    public void copyTable(IcebergCopyTableParam icebergCopyTableParam) throws Exception{
        IcebergTableParam sourceTable = icebergCopyTableParam.getSourceTable();
        IcebergTableParam dstTable = icebergCopyTableParam.getDstTable();

        String sourceTableName = ""+sourceTable.getId()+"."+sourceTable.getDatabaseName()+"."+sourceTable.getTableName();
        String dstTableName = ""+dstTable.getId()+"."+dstTable.getDatabaseName()+"."+dstTable.getTableName();

        PDIcebergUtils sourceIcebergUtils = PDIcebergUtils.build(sourceTable.getId(), sourceTable.getTypes(),
                sourceTable.getHiveurl(), sourceTable.getHdfsurl(),hadoopUser,uploadHiveFilePath);
        if (!sourceIcebergUtils.isExitTable(sourceTable.getDatabaseName(),sourceTable.getTableName())) {
            throw new Exception("来源表已不存在，请重新选择");
        }
        PDIcebergUtils dstIcebergUtils = PDIcebergUtils.build(dstTable.getId(), dstTable.getTypes(),
                dstTable.getHiveurl(), dstTable.getHdfsurl(),hadoopUser,uploadHiveFilePath);
        if(dstIcebergUtils.isExitTable(dstTable.getDatabaseName(),dstTable.getTableName())){
            throw new Exception("复制的目标表已存在存在，请重新填写目标表");
        }

        String showSourceCreateTableSql = "show create table "+sourceTableName;
        PDIcebergSparkUtils pdIcebergSparkUtils =
                PDIcebergSparkUtils.build(sourceTable.getId(), sourceTable.getTypes(), sourceTable.getHiveurl(),
                sourceTable.getHdfsurl(), sourceTable.getDatabaseName(),hadoopUser);
        PDIcebergSparkUtils dstPDIcebergSparkUtils =
                PDIcebergSparkUtils.build(dstTable.getId(), dstTable.getTypes(), dstTable.getHiveurl(),
                dstTable.getHdfsurl(), dstTable.getDatabaseName(),hadoopUser);

        String createDstTableSql = pdIcebergSparkUtils.executeSql(showSourceCreateTableSql).get(0).
                get("createtab_stmt").toString().split("LOCATION")[0].replace(sourceTableName,dstTableName);
        dstPDIcebergSparkUtils.executeSql(createDstTableSql);

        if (icebergCopyTableParam.getIsData()) {
            String copyDataSql = "insert into "+dstTableName+" select * from "+sourceTableName;
            dstPDIcebergSparkUtils.executeSql(copyDataSql);
        }

    }

}
