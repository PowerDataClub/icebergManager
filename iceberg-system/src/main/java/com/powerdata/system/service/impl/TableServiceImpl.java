package com.powerdata.system.service.impl;

import cn.hutool.core.thread.ThreadUtil;
import com.powerdata.common.enums.IcebergBaseType;
import com.powerdata.common.utils.bean.table.TableColumnsBean;
import com.powerdata.common.utils.bean.table.TableMetricsBean;
import com.powerdata.common.utils.bean.table.TablePartitionKeysBean;
import com.powerdata.common.utils.bean.table.TableTransactionsBean;
import com.powerdata.common.utils.excel.ExcelSheetData;
import com.powerdata.common.utils.excel.ExcelXlsReader;
import com.powerdata.common.utils.excel.ExcelXlsxReader;
import com.powerdata.common.utils.iceberg.PDHdfsUtils;
import com.powerdata.common.utils.iceberg.PDIcebergSparkUtils;
import com.powerdata.common.utils.iceberg.PDIcebergUtils;
import com.powerdata.system.domain.param.*;
import com.powerdata.system.service.ITableService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorService;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * @author dinghao
 * @version 1.0
 * @description
 * @date 2023/6/12 16:52
 */
@Service
public class TableServiceImpl implements ITableService {
    @Value(value = "${icebergManager.hadoopUser}")
    private String hadoopUser;
    @Value(value = "${icebergManager.hiveConf}")
    private String uploadHiveFilePath;

    @Override
    public List<String> tableList(IcebergCatalogParam icebergCatalogParam) throws Exception {
        String warehouse = icebergCatalogParam.getHdfsurl();
        String types = icebergCatalogParam.getTypes();
        String hiveUrl = icebergCatalogParam.getHiveurl();
        return PDIcebergUtils.build(icebergCatalogParam.getId(),types,hiveUrl,warehouse,hadoopUser,uploadHiveFilePath)
                .listTable(icebergCatalogParam.getDatabaseName());
    }

    @Override
    public TableMetricsBean getTableMetrics(IcebergCatalogParam icebergCatalogParam) throws Exception {
        String warehouse = icebergCatalogParam.getHdfsurl();
        String types = icebergCatalogParam.getTypes();
        String hiveUrl = icebergCatalogParam.getHiveurl();
        return PDIcebergUtils.build(icebergCatalogParam.getId(),types,hiveUrl,warehouse,hadoopUser,uploadHiveFilePath)
                .getTableMetrics(icebergCatalogParam.getDatabaseName(), icebergCatalogParam.getTableName());
    }

    @Override
    public List<TableColumnsBean> getColumnsOfTable(IcebergCatalogParam icebergCatalogParam) throws Exception {
        String warehouse = icebergCatalogParam.getHdfsurl();
        String types = icebergCatalogParam.getTypes();
        String hiveUrl = icebergCatalogParam.getHiveurl();
        return PDIcebergUtils.build(icebergCatalogParam.getId(),types,hiveUrl,warehouse,hadoopUser,uploadHiveFilePath)
                .getColumnsOfTable(icebergCatalogParam.getDatabaseName(), icebergCatalogParam.getTableName());
    }

    @Override
    public List<TablePartitionKeysBean> getPartitionMessage(IcebergCatalogParam icebergCatalogParam) throws Exception {
        String warehouse = icebergCatalogParam.getHdfsurl();
        String types = icebergCatalogParam.getTypes();
        String hiveUrl = icebergCatalogParam.getHiveurl();
        return PDIcebergUtils.build(icebergCatalogParam.getId(),types,hiveUrl,warehouse,hadoopUser,uploadHiveFilePath)
                .getPartitionMessage(icebergCatalogParam.getDatabaseName(), icebergCatalogParam.getTableName());
    }

    @Override
    public List<TableTransactionsBean> getTransactionsMessage(IcebergCatalogParam icebergCatalogParam) throws Exception {
        String warehouse = icebergCatalogParam.getHdfsurl();
        String types = icebergCatalogParam.getTypes();
        String hiveUrl = icebergCatalogParam.getHiveurl();
        return PDIcebergUtils.build(icebergCatalogParam.getId(),types,hiveUrl,warehouse,hadoopUser,uploadHiveFilePath)
                .getTransactionsMessage(icebergCatalogParam.getDatabaseName(), icebergCatalogParam.getTableName());
    }

    @Override
    public Map<String, Object> getTableData(IcebergCatalogParam icebergCatalogParam) throws Exception {
        Integer pageNum = getNum(icebergCatalogParam.getPageNum(), 1);
        Integer pageSize = getNum(icebergCatalogParam.getPageSize(), 10);
        String warehouse = icebergCatalogParam.getHdfsurl();
        String types = icebergCatalogParam.getTypes();
        String hiveUrl = icebergCatalogParam.getHiveurl();
        return PDIcebergUtils.build(icebergCatalogParam.getId(),types,hiveUrl,warehouse,hadoopUser,uploadHiveFilePath)
                .getTableData(icebergCatalogParam.getDatabaseName(), icebergCatalogParam.getTableName(), pageSize, pageNum);
    }

    @Override
    public Map<String, Object> getDataBySnapshotId(IcebergCatalogParam icebergCatalogParam) throws Exception {
        Integer pageNum = getNum(icebergCatalogParam.getPageNum(), 1);
        Integer pageSize = getNum(icebergCatalogParam.getPageSize(), 10);
        String warehouse = icebergCatalogParam.getHdfsurl();
        String types = icebergCatalogParam.getTypes();
        String hiveUrl = icebergCatalogParam.getHiveurl();
        return PDIcebergUtils.build(icebergCatalogParam.getId(),types,hiveUrl,warehouse,hadoopUser,uploadHiveFilePath)
                .getTableData(icebergCatalogParam.getDatabaseName(), icebergCatalogParam.getTableName()
                        , icebergCatalogParam.getSnapshotId(), pageSize, pageNum);
    }

    @Override
    public Map<String, Object> getDataByTime(IcebergCatalogParam icebergCatalogParam) throws Exception {
        Integer pageNum = getNum(icebergCatalogParam.getPageNum(), 1);
        Integer pageSize = getNum(icebergCatalogParam.getPageSize(), 10);
        String warehouse = icebergCatalogParam.getHdfsurl();
        String types = icebergCatalogParam.getTypes();
        String hiveUrl = icebergCatalogParam.getHiveurl();
        return PDIcebergUtils.build(icebergCatalogParam.getId(),types,hiveUrl,warehouse,hadoopUser,uploadHiveFilePath)
                .getTableDataBeforeTime(icebergCatalogParam.getDatabaseName(), icebergCatalogParam.getTableName()
                        , icebergCatalogParam.getAsOfTime(), pageSize, pageNum);
    }

    @Override
    public void setCurrentSnapshot(IcebergCatalogParam icebergCatalogParam) throws Exception {
        String warehouse = icebergCatalogParam.getHdfsurl();
        String types = icebergCatalogParam.getTypes();
        String hiveUrl = icebergCatalogParam.getHiveurl();
        PDIcebergUtils.build(icebergCatalogParam.getId(),types,hiveUrl,warehouse,hadoopUser,uploadHiveFilePath)
                .setCurrentSnapshot(icebergCatalogParam.getDatabaseName(),
                icebergCatalogParam.getTableName(), icebergCatalogParam.getSnapshotId());
    }

    @Override
    public void rollbackSnapshot(IcebergCatalogParam icebergCatalogParam) throws Exception {
        String warehouse = icebergCatalogParam.getHdfsurl();
        String types = icebergCatalogParam.getTypes();
        String hiveUrl = icebergCatalogParam.getHiveurl();
        PDIcebergUtils.build(icebergCatalogParam.getId(),types,hiveUrl,warehouse,hadoopUser,uploadHiveFilePath)
                .rollbackSnapshot(icebergCatalogParam.getDatabaseName(),
                icebergCatalogParam.getTableName(), icebergCatalogParam.getSnapshotId());
    }

    @Override
    public void cherryPickSnapshot(IcebergCatalogParam icebergCatalogParam) throws Exception {
        String warehouse = icebergCatalogParam.getHdfsurl();
        String types = icebergCatalogParam.getTypes();
        String hiveUrl = icebergCatalogParam.getHiveurl();
        PDIcebergUtils.build(icebergCatalogParam.getId(),types,hiveUrl,warehouse,hadoopUser,uploadHiveFilePath)
                .cherryPickSnapshot(icebergCatalogParam.getDatabaseName(),
                icebergCatalogParam.getTableName(), icebergCatalogParam.getSnapshotId());
    }

    @Override
    public void addData(IcebergCatalogParam icebergCatalogParam) throws Exception {
        String warehouse = icebergCatalogParam.getHdfsurl();
        String types = icebergCatalogParam.getTypes();
        String hiveUrl = icebergCatalogParam.getHiveurl();
        PDIcebergUtils.build(icebergCatalogParam.getId(),types,hiveUrl,warehouse,hadoopUser,uploadHiveFilePath)
                .addTableData(icebergCatalogParam.getDatabaseName(),
                icebergCatalogParam.getTableName(), icebergCatalogParam.getAddData());
    }

    @Override
    public Map<String, Object> getAppendSnapshotTableData(IcebergCatalogParam icebergCatalogParam) throws Exception {
        Integer pageNum = getNum(icebergCatalogParam.getPageNum(), 1);
        Integer pageSize = getNum(icebergCatalogParam.getPageSize(), 10);
        String warehouse = icebergCatalogParam.getHdfsurl();
        String types = icebergCatalogParam.getTypes();
        String hiveUrl = icebergCatalogParam.getHiveurl();
        return PDIcebergUtils.build(icebergCatalogParam.getId(),types,hiveUrl,warehouse,hadoopUser,uploadHiveFilePath)
                .getAppendsTableData(icebergCatalogParam.getDatabaseName(), icebergCatalogParam.getTableName()
                        , icebergCatalogParam.getSnapshotId(), pageSize, pageNum);
    }

    @Override
    public Map<String, Object> getBetweenSnapshotTableData(IcebergCatalogParam icebergCatalogParam) throws Exception {
        Integer pageNum = getNum(icebergCatalogParam.getPageNum(), 1);
        Integer pageSize = getNum(icebergCatalogParam.getPageSize(), 10);
        String warehouse = icebergCatalogParam.getHdfsurl();
        String types = icebergCatalogParam.getTypes();
        String hiveUrl = icebergCatalogParam.getHiveurl();
        return PDIcebergUtils.build(icebergCatalogParam.getId(),types,hiveUrl,warehouse,hadoopUser,uploadHiveFilePath)
                .getAppendsTableData(icebergCatalogParam.getDatabaseName(), icebergCatalogParam.getTableName()
                        , icebergCatalogParam.getSnapshotId(), icebergCatalogParam.getToSnapshotId(), pageSize, pageNum);
    }

    @Override
    public void deleteTable(IcebergCatalogParam icebergCatalogParam) throws Exception {
        String warehouse = icebergCatalogParam.getHdfsurl();
        String types = icebergCatalogParam.getTypes();
        String hiveUrl = icebergCatalogParam.getHiveurl();
        PDIcebergUtils.build(icebergCatalogParam.getId(),types,hiveUrl,warehouse,hadoopUser,uploadHiveFilePath)
                .deleteTable(icebergCatalogParam.getDatabaseName(), icebergCatalogParam.getTableName());
    }

    @Override
    public void renameTable(IcebergCatalogParam icebergCatalogParam) throws Exception {
        String warehouse = icebergCatalogParam.getHdfsurl();
        String types = icebergCatalogParam.getTypes();
        String hiveUrl = icebergCatalogParam.getHiveurl();
        PDIcebergUtils.build(icebergCatalogParam.getId(),types,hiveUrl,warehouse,hadoopUser,uploadHiveFilePath)
                .renameTable(icebergCatalogParam.getDatabaseName(), icebergCatalogParam.getTableName(),
                        icebergCatalogParam.getDatabaseName(), icebergCatalogParam.getNewTableName());
    }

    @Override
    public void moveTable(IcebergCatalogParam icebergCatalogParam) throws Exception {
        String warehouse = icebergCatalogParam.getHdfsurl();
        String types = icebergCatalogParam.getTypes();
        String hiveUrl = icebergCatalogParam.getHiveurl();
        PDIcebergUtils.build(icebergCatalogParam.getId(),types,hiveUrl,warehouse,hadoopUser,uploadHiveFilePath)
                .renameTable(icebergCatalogParam.getDatabaseName(), icebergCatalogParam.getTableName(),
                        icebergCatalogParam.getNewDatabaseName(), icebergCatalogParam.getNewTableName());
    }

    private int getNum(Integer pageNum, int i) {
        return ObjectUtils.allNotNull(pageNum) ? pageNum : i;
    }

    @Override
    public void createTable(IcebergTableParam icebergTableParam) throws Exception {

        List<IcebergPartitionDto> partitionParam = icebergTableParam.getPartitionParams();
        ObjectMapper objectMapper = new ObjectMapper();
        List<Map<String, String>> partitionMapList = new ArrayList<>();
        for (IcebergPartitionDto partition : partitionParam) {
            String s = objectMapper.writeValueAsString(partition);
            partitionMapList.add(objectMapper.readValue(s, Map.class));
        }

        List<IcebergColumnDto> columnDtos = icebergTableParam.getColumnDtos();
        List<Map<String, String>> columMapList = new ArrayList<>();
        for (IcebergColumnDto column : columnDtos) {
            String s = objectMapper.writeValueAsString(column);
            columMapList.add(objectMapper.readValue(s, Map.class));
        }
        String warehouse = icebergTableParam.getHdfsurl();
        String types = icebergTableParam.getTypes();
        String hiveUrl = icebergTableParam.getHiveurl();
        PDIcebergUtils.build(icebergTableParam.getId(),types,hiveUrl,warehouse,hadoopUser,uploadHiveFilePath)
                .createTable(icebergTableParam.getDatabaseName(), icebergTableParam.getTableName(), columMapList, partitionMapList);
    }

    @Override
    public void updateTable(IcebergTableParam icebergTableParam) throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        List<IcebergPartitionDto> partitionParam = icebergTableParam.getPartitionParams();
        List<IcebergColumnDto> columnDtos = icebergTableParam.getColumnDtos();
        List<Map<String, String>> columMapList = new ArrayList<>();
        for (IcebergColumnDto column : columnDtos) {
            String s = objectMapper.writeValueAsString(column);
            columMapList.add(objectMapper.readValue(s, Map.class));
        }
        List<Map<String, String>> partitionMapList = new ArrayList<>();
        for (IcebergPartitionDto partition : partitionParam) {
            String s = objectMapper.writeValueAsString(partition);
            partitionMapList.add(objectMapper.readValue(s, Map.class));
        }

        String warehouse = icebergTableParam.getHdfsurl();
        String types = icebergTableParam.getTypes();
        String hiveUrl = icebergTableParam.getHiveurl();
        PDIcebergUtils.build(icebergTableParam.getId(),types,hiveUrl,warehouse,hadoopUser,uploadHiveFilePath)
                .updateTable(icebergTableParam.getDatabaseName(), icebergTableParam.getTableName(), columMapList,partitionMapList);
    }

    @Override
    public IcebergTableParam queryTableInfo(IcebergTableParam icebergTableParam) throws Exception {

        IcebergTableParam result = new IcebergTableParam();

        String types = icebergTableParam.getTypes();
        String hiveUrl = icebergTableParam.getHiveurl();
        String warehouse = icebergTableParam.getHdfsurl();
        String databaseName = icebergTableParam.getDatabaseName();
        String tableName = icebergTableParam.getTableName();
        result.setHdfsurl(warehouse);
        result.setDatabaseName(databaseName);
        result.setTableName(tableName);
        PDIcebergUtils build = PDIcebergUtils.build(icebergTableParam.getId(),types,hiveUrl,warehouse,hadoopUser,uploadHiveFilePath);
        List<TableColumnsBean> columnsOfTable = build.getColumnsOfTable(databaseName, tableName);

        List<IcebergColumnDto> columnObjects = new ArrayList<>();
        for (TableColumnsBean tableColumnsBean : columnsOfTable) {
            IcebergColumnDto columnDto = new IcebergColumnDto();
            columnDto.setColumnName(tableColumnsBean.getColumnName());
            columnDto.setComment(tableColumnsBean.getComment());
            columnDto.setIsNullable(tableColumnsBean.getIsNullable());
            String dataType = tableColumnsBean.getDataType();
            IcebergBaseType type = IcebergBaseType.getType(dataType);
            if (type != null) {
                columnDto.setDataType(type.name());
            } else if (dataType.contains("decimal")) {
                String pattern = "\\((.*?)\\)";
                Pattern compile = Pattern.compile(pattern);
                Matcher matcher = compile.matcher(dataType);
                String match = "";
                while (matcher.find()) {
                    match = matcher.group(1);
                }
                String[] split = match.split(",");
                columnDto.setPrecision(split[0].trim());
                columnDto.setScale(split[1].trim());
                columnDto.setDataType("Decimal");
            } else if (dataType.contains("timestamp")) {
                columnDto.setDataType("Timestamp");
                if (dataType.endsWith("tz")) {
                    columnDto.setIsWithZone("1");
                } else {
                    columnDto.setIsWithZone("0");
                }
            } else if (dataType.contains("list")) {
                columnDto.setDataType("List");
                String substring = dataType.substring(dataType.indexOf("<") + 1, dataType.indexOf(">"));
                IcebergBaseType subType = IcebergBaseType.getType(substring);
                columnDto.setValueType(subType.name());
            }else if (dataType.contains("map")) {
                columnDto.setDataType("Map");
                String substring = dataType.substring(dataType.indexOf("<") + 1, dataType.indexOf(">"));
                String[] split = substring.split(",");
                IcebergBaseType keyType = IcebergBaseType.getType(split[0]);
                IcebergBaseType valueType = IcebergBaseType.getType(split[1]);
                columnDto.setValueType(valueType.name());
                columnDto.setKeyType(keyType.name());
            }
            columnObjects.add(columnDto);
        }

        result.setColumnDtos(columnObjects);

        List<IcebergPartitionDto> partitionObjects = new ArrayList<>();
        List<TablePartitionKeysBean> partitionMessage = build.getPartitionMessage(databaseName, tableName);
        Map<String, String> collect = columnObjects.stream().collect(Collectors.toMap(IcebergColumnDto::getColumnName, IcebergColumnDto::getDataType));
        for (TablePartitionKeysBean keysBean : partitionMessage)
        {
            IcebergPartitionDto partitionObject = new IcebergPartitionDto();
            partitionObject.setTargetName(keysBean.getField());
            partitionObject.setType(keysBean.getTransform());
            partitionObject.setSourceName(keysBean.getSourceField());
            // 将分区字段类型给到
            partitionObject.setDateType(collect.get(partitionObject.getSourceName()));
            partitionObjects.add(partitionObject);
        }

        result.setPartitionParams(partitionObjects);
        return result;
    }

    @Override
    public void deleteData(IcebergCatalogParam icebergCatalogParam) throws Exception {
        String warehouse = icebergCatalogParam.getHdfsurl();
        String types = icebergCatalogParam.getTypes();
        String hiveUrl = icebergCatalogParam.getHiveurl();
        PDIcebergUtils.build(icebergCatalogParam.getId(),types,hiveUrl,warehouse,hadoopUser,uploadHiveFilePath)
                .deleteTableData(icebergCatalogParam.getDatabaseName(),
                icebergCatalogParam.getTableName(), icebergCatalogParam.getAddData());
    }

    @Override
    public void importDataToTable(String id,String type,String hiveUrl,String warehouse,
                                  String databaseName,String tableName, MultipartFile file) throws Exception {
        PDIcebergUtils pdIcebergUtils = PDIcebergUtils.build(id,type,hiveUrl,warehouse,hadoopUser,uploadHiveFilePath);
        List<TableColumnsBean> columnsOfTable = pdIcebergUtils.getColumnsOfTable(databaseName, tableName);
        ExecutorService executorService = ThreadUtil.newExecutor(10, 20, 20);
        CompletionService<Object> uploadFileCompletionService = ThreadUtil.newCompletionService(executorService);
        try {
            String filename = file.getOriginalFilename();
            // parse file
            List<ExcelSheetData> excelSheetDataList = parseDataExcel(Objects.requireNonNull(filename), file);
            for (ExcelSheetData excelSheetData : excelSheetDataList) {
                uploadFileCompletionService.submit(new Runnable() {
                    @Override
                    public void run() {
                        ArrayList<HashMap<String, String>> addDataList = new ArrayList<>();
                        for (List<String> datum : excelSheetData.getData()) {
                            if (datum.size() != columnsOfTable.size()) {
                                try {
                                    throw new Exception("导入excel列数和表的列数不一致，无法导入");
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            }
                            HashMap<String, String> midDataMap = new HashMap<>();
                            for (int i = 0; i < datum.size(); i++) {
                                midDataMap.put(columnsOfTable.get(i).getColumnName(),datum.get(i));
                            }
                            addDataList.add(midDataMap);
                        }
                        pdIcebergUtils.addTableData(databaseName, tableName, addDataList);
                    }}, "success");
            }
        } catch (Exception e) {
            throw new Exception(e.getMessage());
        }
        uploadFileCompletionService.take();
        executorService.shutdown();
    }

    @Override
    public Map<String, Object> getMetadataFiles(IcebergCatalogParam icebergCatalogParam) throws Exception {
        Integer pageNum = getNum(icebergCatalogParam.getPageNum(), 1);
        Integer pageSize = getNum(icebergCatalogParam.getPageSize(), 10);
        String warehouse = icebergCatalogParam.getHdfsurl();
        String types = icebergCatalogParam.getTypes();
        String hiveUrl = icebergCatalogParam.getHiveurl();
        String databaseName = icebergCatalogParam.getDatabaseName();
        String tableName = icebergCatalogParam.getTableName();
        String fileName = icebergCatalogParam.getFileName();
        return PDIcebergUtils.build(icebergCatalogParam.getId(),types,hiveUrl,warehouse,hadoopUser,uploadHiveFilePath)
                .getMetadataFiles(databaseName,tableName,fileName,pageSize,pageNum);
    }

    @Override
    public String getMetadata(IcebergCatalogParam icebergCatalogParam) throws Exception {
        String warehouse = icebergCatalogParam.getHdfsurl();
        String types = icebergCatalogParam.getTypes();
        String hiveUrl = icebergCatalogParam.getHiveurl();
        String databaseName = icebergCatalogParam.getDatabaseName();
        String tableName = icebergCatalogParam.getTableName();
        String fileName = icebergCatalogParam.getFileName();
        return PDIcebergUtils.build(icebergCatalogParam.getId(),types,hiveUrl,warehouse,hadoopUser,uploadHiveFilePath)
                .getMetadata(databaseName,tableName,fileName);
    }

    @Override
    public void mergeSmallFile(IcebergCatalogParam icebergCatalogParam) throws Exception {
        String warehouse = icebergCatalogParam.getHdfsurl();
        String types = icebergCatalogParam.getTypes();
        String hiveUrl = icebergCatalogParam.getHiveurl();
        PDIcebergUtils.build(icebergCatalogParam.getId(),types,hiveUrl,warehouse,hadoopUser,uploadHiveFilePath)
                .mergeSmallFile(icebergCatalogParam.getDatabaseName(),icebergCatalogParam.getTableName());

    }

    @Override
    public Map<String,Object> tableList2(IcebergCatalogParam icebergCatalogParam) throws Exception {
        HashMap<String, Object> resultMap = new HashMap<>();
        String catalogId = icebergCatalogParam.getId();
        String warehouse = icebergCatalogParam.getHdfsurl();
        String databaseName = icebergCatalogParam.getDatabaseName();
        if("hive".equals(icebergCatalogParam.getTypes())){
            databaseName = databaseName+".db";
        }
        String tableName = icebergCatalogParam.getTableName();
        int pageSize = ObjectUtils.isEmpty(icebergCatalogParam.getPageSize()) ? 10:icebergCatalogParam.getPageSize();
        int pageNum = ObjectUtils.isEmpty(icebergCatalogParam.getPageNum()) ? 1:icebergCatalogParam.getPageNum();
        List<String> tables = PDHdfsUtils.build(catalogId, warehouse,hadoopUser).getDirs(databaseName, tableName);
        resultMap.put("total",tables.size());
        resultMap.put("data",tables.stream().skip((long) (pageNum - 1) *pageSize).limit(pageSize).collect(Collectors.toList()));
        return resultMap;
    }

    @Override
    public void tableDataToFile(TableToDataParam tableToDataParam) throws Exception {
        IcebergTableParam icebergTableParam = tableToDataParam.getIcebergTableParam();
        IcebergFileParam icebergFileParam = tableToDataParam.getIcebergFileParam();
        String splitStr = tableToDataParam.getSplitStr();
        String isHead = tableToDataParam.getIsHead();
        String filePath = icebergFileParam.getFilePath();

        FSDataOutputStream fileOutPutStream =
                PDHdfsUtils.build(icebergFileParam.getCatalogId(), icebergFileParam.getHdfsUrl(),hadoopUser)
                        .getFileOutPutStream(filePath);
        addHDFSDataFromTable(fileOutPutStream,icebergTableParam,splitStr,isHead);
        //整体转储（大表有内存溢出的风险）
//        ArrayList<String> dataList = getAllDataWithSplit(icebergTableParam,splitStr,isHead);
//        listWriteToHdfs(icebergFileParam,dataList);
    }

    private void addHDFSDataFromTable(FSDataOutputStream fileOutPutStream, IcebergTableParam icebergTableParam,
                                      String splitStr, String isHead) throws Exception {
        String warehouse = icebergTableParam.getHdfsurl();
        String types = icebergTableParam.getTypes();
        String hiveUrl = icebergTableParam.getHiveurl();
        PDIcebergUtils.build(icebergTableParam.getId(),types,hiveUrl,warehouse,hadoopUser,uploadHiveFilePath)
                .tableDataToFileWithBranch(icebergTableParam.getDatabaseName(), icebergTableParam.getTableName(),
                        fileOutPutStream,splitStr,isHead);
    }


    @Override
    public void fileToTableData(TableToDataParam tableToDataParam) throws Exception {
        IcebergTableParam icebergTableParam = tableToDataParam.getIcebergTableParam();
        IcebergFileParam icebergFileParam = tableToDataParam.getIcebergFileParam();
        String splitStr = tableToDataParam.getSplitStr();
        String isHead = tableToDataParam.getIsHead();

        PDIcebergUtils pdIcebergUtils = PDIcebergUtils.build(icebergTableParam.getId(),
                icebergTableParam.getTypes(), icebergTableParam.getHiveurl(), icebergTableParam.getHdfsurl(),hadoopUser,uploadHiveFilePath);

        FSDataInputStream fileInputStream = getFileInputStream(icebergFileParam);

        pdIcebergUtils.addTableDataFromHDFSFile(icebergTableParam.getDatabaseName(),icebergTableParam.getTableName()
                ,new InputStreamReader(fileInputStream),splitStr,isHead,null);

    }

    @Override
    public void clearTableData(IcebergTableParam icebergTableParam) throws Exception {
        String id = icebergTableParam.getId();
        String warehouse = icebergTableParam.getHdfsurl();
        String types = icebergTableParam.getTypes();
        String hiveUrl = icebergTableParam.getHiveurl();
        String databaseName = icebergTableParam.getDatabaseName();
        String tableName = icebergTableParam.getTableName();
        PDIcebergSparkUtils.build(id,types,hiveUrl,warehouse,null,hadoopUser)
                .executeSql("delete from "+id+"."+databaseName+"."+tableName);
    }

    private FSDataInputStream getFileInputStream(IcebergFileParam icebergFileParam) throws Exception{
        String catalogId = icebergFileParam.getCatalogId();
        String warehouse = icebergFileParam.getHdfsUrl();
        String filePath = icebergFileParam.getFilePath();
        return PDHdfsUtils.build(catalogId, warehouse,hadoopUser).getFileInputStream(filePath);
    }

    @Override
    public void updateData(UpdateDataParam updateDataParam) throws Exception {
        String warehouse = updateDataParam.getHdfsurl();
        String types = updateDataParam.getTypes();
        String hiveUrl = updateDataParam.getHiveurl();
        PDIcebergUtils.build(updateDataParam.getId(),types,hiveUrl,warehouse,hadoopUser,uploadHiveFilePath)
                .updateData(updateDataParam.getDatabaseName(), updateDataParam.getTableName(),
                        updateDataParam.getOldData(),updateDataParam.getNewData());
    }

    private void listWriteToHdfs(IcebergFileParam icebergFileParam, ArrayList<String> dataList) throws Exception {
        String catalogId = icebergFileParam.getCatalogId();
        String warehouse = icebergFileParam.getHdfsUrl();
        String filePath = icebergFileParam.getFilePath();
        PDHdfsUtils.build(catalogId, warehouse,hadoopUser).writeToFile(filePath,dataList);
    }

    private ArrayList<String> getAllDataWithSplit(IcebergTableParam icebergTableParam, String splitStr,String isHead) throws Exception {
        String warehouse = icebergTableParam.getHdfsurl();
        String types = icebergTableParam.getTypes();
        String hiveUrl = icebergTableParam.getHiveurl();
        return PDIcebergUtils.build(icebergTableParam.getId(),types,hiveUrl,warehouse,hadoopUser,uploadHiveFilePath)
                .getAllTableData(icebergTableParam.getDatabaseName(), icebergTableParam.getTableName(),splitStr,isHead);
    }

    public static List<ExcelSheetData> parseDataExcel(String filename, MultipartFile file) throws Exception {
        InputStream inputStream = file.getInputStream();
        List<ExcelSheetData> excelSheetDataList = new ArrayList<>();
        String suffix = filename.substring(filename.lastIndexOf(".") + 1);
        if (StringUtils.equalsIgnoreCase(suffix, "xls")) {
            ExcelXlsReader excelXlsReader = new ExcelXlsReader();
            excelXlsReader.process(inputStream);
            excelSheetDataList = excelXlsReader.totalSheets;
        }
        if (StringUtils.equalsIgnoreCase(suffix, "xlsx")) {
            ExcelXlsxReader excelXlsxReader = new ExcelXlsxReader();
            excelXlsxReader.process(inputStream);
            excelSheetDataList = excelXlsxReader.totalSheets;
        }

        inputStream.close();

        return excelSheetDataList;
    }
}
