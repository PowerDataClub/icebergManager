package com.powerdata.common.utils.iceberg;

import com.powerdata.common.utils.bean.table.CreateOrUpdateColumn;
import com.powerdata.common.utils.bean.table.HiveCreateOrUpdateTableRequest;
import com.powerdata.common.utils.bean.table.OldNewValue;
import net.sf.jsqlparser.expression.CastExpression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author dinghao
 * @version 1.0
 * @description
 * @date 2023/8/30 13:36
 */
public class PDHiveUtils {

    private static final Logger logger = LoggerFactory.getLogger(PDIcebergSparkUtils.class);

    private static Map<String, String> showTableModule = new HashMap<>();
    private static Set<String> columnTypeOfDefaultCast = new HashSet<>();

    public static HashMap<String, PDHiveUtils> pdHiveUtils = new HashMap<>();
    private Connection connection=null;
    private PDHiveUtils(String ip,String hiveUser) throws Exception{
        Class.forName("com.cloudera.hive.jdbc4.HS2Driver");
        String url = "jdbc:hive2://" + ip + ":10000";
        connection = DriverManager.getConnection(url,hiveUser,null);
    }

    public static PDHiveUtils build(String ip,String hiveUser) throws Exception {
        if(!pdHiveUtils.containsKey(ip)){
            pdHiveUtils.put(ip,new PDHiveUtils(ip,hiveUser));
        }
        return pdHiveUtils.get(ip);
    }

    public List<String> getAllTableNames(String databaseName) throws Exception {
        ArrayList<String> results = new ArrayList<>();
        String showTableSql = "show tables";
        Statement statement = connection.createStatement();
        statement.execute("use "+databaseName);
        ResultSet resultSet = statement.executeQuery(showTableSql);
        while (resultSet.next()) {
            results.add(resultSet.getString("tab_name").trim());
        }
        return results;
    }

    public Map<String,String> getTableMessage(String table) throws Exception {
        String showTableSql = "desc formatted "+table;
        String showCreateSql = "show create table "+table;
        Statement statement = connection.createStatement();
        HashMap<String, String> resultMap = new HashMap<>();
        try {
            ResultSet resultSet = statement.executeQuery(showTableSql);
            // 解析resultset获得原始的分块数据
            Map<String, Object> mapResult = getTableInfo(resultSet, table.split("\\.")[1]);
            HiveCreateOrUpdateTableRequest result = resolveJsonTable(mapResult);
            String splitStr = result.getFieldTerminated();
            String hdfsLocation = result.getHdfsLocation();
            ResultSet resultSet1 = statement.executeQuery(showCreateSql);
            String createSql = "";
            while (resultSet1.next()) {
                createSql = createSql+" "+resultSet1.getString("createtab_stmt").trim();
            }
            String createIcebergSql = (createSql.split("ROW FORMAT SERDE"))[0]
                    .replaceAll("\\) PARTITIONED BY \\(",",")
                    + " using iceberg";
            resultMap.put("splitStr",splitStr);
            resultMap.put("filePath",hdfsLocation);
            resultMap.put("createSql",createIcebergSql);
        }catch (Exception e){
            e.printStackTrace();
            throw new Exception(e.getMessage());
        }finally {
            statement.close();
        }
        return resultMap;
    }

    private HiveCreateOrUpdateTableRequest resolveJsonTable(Map<String, Object> map) {
        Map<String, String> detailTableInformation = (Map<String, String>) map.get("detailTableInformation");
        Map<String, String> tableParameters = (Map<String, String>) map.get("tableParameters");
        Map<String, String> storageInformation = (Map<String, String>) map.get("storageInformation");
        Map<String, String> storageDescParams = (Map<String, String>) map.get("storageDescParams");
        Map<String, Map<String, String>> constraints = (Map<String, Map<String, String>>) map.get("constraints");
        List<Map<String, String>> columns = (List<Map<String, String>>) map.get("columns");
        List<Map<String, String>> partitions = (List<Map<String, String>>) map.get("partitions");

        HiveCreateOrUpdateTableRequest response = new HiveCreateOrUpdateTableRequest();

        String tableComment = tableParameters.getOrDefault("comment", "");
        OldNewValue<String> tableCommentObj = new OldNewValue<>(tableComment, tableComment);
        response.setTableComment(tableCommentObj);

        String tableType = detailTableInformation.getOrDefault("Table Type", "").replace("_TABLE", "");
        response.setTableType(tableType);

        String inputFormat = storageInformation.getOrDefault("InputFormat", "");
        String fileFormat = getFileFormat(inputFormat);
        OldNewValue<String> fileFormatObj = new OldNewValue<>(fileFormat, fileFormat);
        response.setFileFormat(fileFormatObj);

        List<CreateOrUpdateColumn> tableColumns = new ArrayList<>();

        columns.forEach(colMap -> {
            CreateOrUpdateColumn column = new CreateOrUpdateColumn();

            String columnName = colMap.get("col_name");
            OldNewValue<String> columnNameObj = new OldNewValue<>(columnName, columnName);
            column.setColumnName(columnNameObj);

            String columnType = colMap.get("data_type");
            OldNewValue<String> columnTypeObj = new OldNewValue<>(columnType, columnType);
            column.setColumnType(columnTypeObj);

            String comment = colMap.get("comment");
            OldNewValue<String> commentObj = new OldNewValue<>(comment, comment);
            column.setColumnComment(commentObj);

            if (constraints.containsKey("notnull") && constraints.get("notnull").containsKey(columnName)) {
                OldNewValue<Boolean> oldNewValue = new OldNewValue<>(true, true);
                oldNewValue.setConstraintName(constraints.get("notnull").get(columnName));
                column.setNotNull(oldNewValue);
            } else {
                column.setNotNull(new OldNewValue<>(false, false));
            }

            if (constraints.containsKey("default") && constraints.get("default").containsKey(columnName)) {
                String defaultValue = constraints.get("default").get(columnName);
                // cast注释处理
                if (StringUtils.isNotBlank(defaultValue) && columnTypeOfDefaultCast.contains(columnType))
                    defaultValue = getCastDefaultValue(defaultValue);

                defaultValue = defaultValue.replace("'", "");

                String constraintName = constraints.get("default").get(columnName + "_constraintName");
                OldNewValue<String> oldNewValue = new OldNewValue<>(defaultValue, defaultValue);
                oldNewValue.setConstraintName(constraintName);
                column.setDefaultValue(oldNewValue);
            } else {
                column.setDefaultValue(new OldNewValue<>("", ""));
            }

            tableColumns.add(column);
        });

        response.setColumns(tableColumns);

        if (partitions.size() > 0) {

            List<CreateOrUpdateColumn> tablePartitions = new ArrayList<>();

            partitions.forEach(colMap -> {
                CreateOrUpdateColumn column = new CreateOrUpdateColumn();

                String columnName = colMap.get("col_name");
                OldNewValue<String> columnNameObj = new OldNewValue<>(columnName, columnName);
                column.setColumnName(columnNameObj);

                String columnType = colMap.get("data_type");
                OldNewValue<String> columnTypeObj = new OldNewValue<>(columnType, columnType);
                column.setColumnType(columnTypeObj);

                String comment = colMap.get("comment");
                OldNewValue<String> commentObj = new OldNewValue<>(comment, comment);
                column.setColumnComment(commentObj);

                tablePartitions.add(column);
            });

            response.setPartitions(tablePartitions);
        }

        String createTime = detailTableInformation.get("CreateTime");
        createTime = getCreateTime(createTime);
        response.setCreateTime(createTime);

        String delim = storageDescParams.getOrDefault("field.delim", "\001");
        response.setFieldTerminated(delim);

        String hdfsLocation = detailTableInformation.getOrDefault("Location", "");
        response.setHdfsLocation(hdfsLocation);

        int numFiles = Integer.parseInt(tableParameters.getOrDefault("numFiles" , "0"));
        response.setHasData(numFiles > 0);

        long totalSize = Long.parseLong(tableParameters.getOrDefault("totalSize", "0")) / 1024L;
        response.setTotalSize(totalSize);

        int numPartitions = Integer.parseInt(tableParameters.getOrDefault("numPartitions", "0"));
        response.setNumPartitions(numPartitions);

        return response;
    }

    private String getFileFormat(String inputFormat) {
        if (inputFormat.contains("Text")) return "TEXTFILE";
        if (inputFormat.contains("Orc")) return "ORC";
        if (inputFormat.contains("Parquet")) return "PARQUET";
        if (inputFormat.contains("RCFile")) return "RCFILE";
        if (inputFormat.contains("Avro")) return "AVRO";

        return "text";
    }

    private String getCastDefaultValue(String castComment) {
        String sql = "select " + castComment;

        String comment = "";
        try {
            net.sf.jsqlparser.statement.Statement statement = CCJSqlParserUtil.parse(sql);
            SelectItem item = ((PlainSelect) ((Select) statement).getSelectBody()).getSelectItems().get(0);
            CastExpression cast = (CastExpression) ((SelectExpressionItem) item).getExpression();
            comment = cast.getLeftExpression().toString();
        } catch (Exception e) {
            logger.error("resolve cast expression [{}] error:", castComment, e);
        }

        return comment;
    }

    private String getCreateTime(String createTime) {
        String ctFormat = "yyyy-MM-dd HH:mm:ss";
        Date date = StringUtils.isBlank(createTime) ? new Date() : new Date(createTime);
        return new SimpleDateFormat(ctFormat).format(date);
    }

    private Map<String, Object> getTableInfo(ResultSet resultSet, String tableName) throws Exception {
        Map<String, Object> result = new HashMap<>();

        Map<String, String> detailTableInformation = new HashMap<>();
        Map<String, String> tableParameters = new HashMap<>();
        Map<String, String> storageInformation = new HashMap<>();
        Map<String, String> storageDescParams = new HashMap<>();
        Map<String, Map<String, String>> constraints = new HashMap<>();
        List<Map<String, String>> columns = new ArrayList<>();
        List<Map<String, String>> partitions = new ArrayList<>();

        String infoModule = "";
        Map<String, String> moduleMap = getShowTableModule();

        // 解析resultset获得原始的分块数据
        while (resultSet.next()) {

            String title = resultSet.getString(1);

            if (("".equals(title) && resultSet.getString(2) == null) || "# Constraints".equals(title)) continue;

            if (moduleMap.containsKey(title)) {
                if ("partition_info".equals(infoModule) && "col_name".equals(moduleMap.get(title))) continue;
                infoModule = moduleMap.get(title);
                logger.info("start record table [{}] module [{}] info", tableName, infoModule);
                continue;
            }

            String key = null;
            String strValue = null;
            switch (infoModule) {
                case "col_name":
                    Map<String, String> map = new HashMap<>();
                    int colNum = resultSet.getMetaData().getColumnCount();
                    for (int col = 0; col < colNum; col++) {
                        String columnName = resultSet.getMetaData().getColumnName(col + 1);
                        String columnValue = resultSet.getString(columnName);
                        map.put(columnName, columnValue);
                    }
                    columns.add(map);
                    break;

                case "table_info":
                    key = resultSet.getString(1).trim().replace(":", "");
                    strValue = resultSet.getString(2).trim();
                    detailTableInformation.put(key, strValue);
                    break;

                case "table_param":
                    key = resultSet.getString(2).trim().replace(":", "");
                    strValue = resultSet.getString(3).trim();
                    tableParameters.put(key, strValue);
                    break;

                case "storage_info":
                    key = resultSet.getString(1).trim().replace(":", "");
                    strValue = resultSet.getString(2).trim();
                    storageInformation.put(key, strValue);
                    break;

                case "storage_desc":
                    key = resultSet.getString(2).trim().replace(":", "");
                    strValue = resultSet.getString(3).trim();
                    storageDescParams.put(key, strValue);
                    break;

                case "not_null_constrain":
                    Map<String, String> notNullMap = constraints.getOrDefault("notnull", new HashMap<>());
                    if ("Table:".equals(title.trim())) resultSet.next();

                    String notNullConstraintName = resultSet.getString(2).trim();
                    resultSet.next();

                    strValue = resultSet.getString(2).trim();
                    notNullMap.put(strValue, notNullConstraintName);

                    constraints.put("notnull", notNullMap);
                    break;

                case "default_constraint":
                    Map<String, String> defaultMap = constraints.getOrDefault("default", new HashMap<>());
                    if ("Table:".equals(title.trim())) resultSet.next();

                    String defaultConstraintName = resultSet.getString(2).trim();
                    resultSet.next();

                    key = resultSet.getString(1).trim().split(":")[1];
                    strValue = resultSet.getString(2).trim();
                    int valueIndex = strValue.indexOf(":");
                    strValue = strValue.substring(valueIndex + 1);
                    defaultMap.put(key, strValue);

                    defaultMap.put(key + "_constraintName", defaultConstraintName);

                    constraints.put("default", defaultMap);
                    break;

                case "partition_info":
                    Map<String, String> partitionMap = new HashMap<>();
                    int partitionColNum = resultSet.getMetaData().getColumnCount();
                    for (int col = 0; col < partitionColNum; col++) {
                        String columnName = resultSet.getMetaData().getColumnName(col + 1);
                        String columnValue = resultSet.getString(columnName);
                        partitionMap.put(columnName, columnValue);
                    }
                    partitions.add(partitionMap);
                    break;

                default:
                    logger.error("unknown module [{}], will support later", infoModule);
            }
        }

        result.put("columns", columns);
        result.put("detailTableInformation", detailTableInformation);
        result.put("tableParameters", tableParameters);
        result.put("storageInformation", storageInformation);
        result.put("storageDescParams", storageDescParams);
        result.put("constraints", constraints);
        result.put("partitions", partitions);

        return result;
    }

    private Map<String, String> getShowTableModule() {
        if (showTableModule.size() > 0) return showTableModule;

        showTableModule.put("# col_name", "col_name");
        showTableModule.put("# Detailed Table Information", "table_info");
        showTableModule.put("Table Parameters:", "table_param");
        showTableModule.put("# Storage Information", "storage_info");
        showTableModule.put("Storage Desc Params:", "storage_desc");
        showTableModule.put("# Not Null Constraints", "not_null_constrain");
        showTableModule.put("# Default Constraints", "default_constraint");
        showTableModule.put("# Partition Information", "partition_info");

        columnTypeOfDefaultCast.add("timestamp");
        columnTypeOfDefaultCast.add("date");
        columnTypeOfDefaultCast.add("binary");

        return showTableModule;
    }

}
