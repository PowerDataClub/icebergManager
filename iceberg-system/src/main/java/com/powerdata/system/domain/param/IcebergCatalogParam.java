package com.powerdata.system.domain.param;

import com.powerdata.system.domain.IcebergCatalog;
import lombok.Data;

import java.util.HashMap;
import java.util.List;

/**
 * @author dinghao
 * @version 1.0
 * @description
 * @date 2023/6/12 13:41
 */
@Data
public class IcebergCatalogParam extends IcebergCatalog {
    private String databaseName;
    private String tableName;
    private String newDatabaseName;
    private String newTableName;
    private Long snapshotId;
    private Long asOfTime;
    private Long toSnapshotId;
    private List<HashMap<String,String>> addData;
    private String execSql;
    private String fileName;

    private Integer pageSize;
    private Integer pageNum;
    private String orderByColumn;
    private String isAsc = "asc";

    private String execType = "spark";
    private String mode="local";
}
