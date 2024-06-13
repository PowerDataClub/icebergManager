package com.powerdata.system.service;

import com.powerdata.common.utils.bean.table.TableColumnsBean;
import com.powerdata.common.utils.bean.table.TableMetricsBean;
import com.powerdata.common.utils.bean.table.TablePartitionKeysBean;
import com.powerdata.common.utils.bean.table.TableTransactionsBean;
import com.powerdata.system.domain.param.IcebergCatalogParam;
import com.powerdata.system.domain.param.IcebergTableParam;
import com.powerdata.system.domain.param.TableToDataParam;
import com.powerdata.system.domain.param.UpdateDataParam;
import org.springframework.web.multipart.MultipartFile;

import java.util.List;
import java.util.Map;

/**
 * @author dinghao
 * @version 1.0
 * @description
 * @date 2023/6/12 16:52
 */
public interface ITableService {
    List<String> tableList(IcebergCatalogParam icebergCatalogParam) throws Exception;

    TableMetricsBean getTableMetrics(IcebergCatalogParam icebergCatalogParam) throws Exception;

    List<TableColumnsBean> getColumnsOfTable(IcebergCatalogParam icebergCatalogParam) throws Exception;

    List<TablePartitionKeysBean> getPartitionMessage(IcebergCatalogParam icebergCatalogParam) throws Exception;

    List<TableTransactionsBean> getTransactionsMessage(IcebergCatalogParam icebergCatalogParam) throws Exception;

    Map<String,Object> getTableData(IcebergCatalogParam icebergCatalogParam) throws Exception;

    Map<String,Object> getDataBySnapshotId(IcebergCatalogParam icebergCatalogParam) throws Exception;

    Map<String,Object> getDataByTime(IcebergCatalogParam icebergCatalogParam) throws Exception;

    void setCurrentSnapshot(IcebergCatalogParam icebergCatalogParam) throws Exception;

    void rollbackSnapshot(IcebergCatalogParam icebergCatalogParam) throws Exception;

    void cherryPickSnapshot(IcebergCatalogParam icebergCatalogParam) throws Exception;

    void addData(IcebergCatalogParam icebergCatalogParam) throws Exception;

    Map<String,Object> getAppendSnapshotTableData(IcebergCatalogParam icebergCatalogParam) throws Exception;

    Map<String,Object> getBetweenSnapshotTableData(IcebergCatalogParam icebergCatalogParam) throws Exception;

    void deleteTable(IcebergCatalogParam icebergCatalogParam) throws Exception;

    void renameTable(IcebergCatalogParam icebergCatalogParam) throws Exception;

    void moveTable(IcebergCatalogParam icebergCatalogParam) throws Exception;

    void createTable(IcebergTableParam icebergTableParam) throws Exception;

    void updateTable(IcebergTableParam icebergTableParam) throws Exception;

    IcebergTableParam queryTableInfo(IcebergTableParam icebergTableParam) throws Exception;

    void deleteData(IcebergCatalogParam icebergCatalogParam) throws Exception;

    void importDataToTable(String id,String type,String hiveUrl,String warehouse,String databaseName,
                           String tableName, MultipartFile file) throws Exception;

    Map<String,Object> getMetadataFiles(IcebergCatalogParam icebergCatalogParam) throws Exception;

    String getMetadata(IcebergCatalogParam icebergCatalogParam) throws Exception;

    void mergeSmallFile(IcebergCatalogParam icebergCatalogParam) throws Exception;

    Map<String,Object> tableList2(IcebergCatalogParam icebergCatalogParam) throws Exception;

    void tableDataToFile(TableToDataParam tableToDataParam) throws Exception;

    void updateData(UpdateDataParam updateDataParam) throws Exception;

    void fileToTableData(TableToDataParam tableToDataParam) throws Exception;

    void clearTableData(IcebergTableParam icebergTableParam) throws Exception;
}
