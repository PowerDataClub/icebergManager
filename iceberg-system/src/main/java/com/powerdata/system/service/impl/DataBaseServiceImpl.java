package com.powerdata.system.service.impl;

import com.powerdata.common.utils.iceberg.PDIcebergUtils;
import com.powerdata.system.domain.param.IcebergCatalogParam;
import com.powerdata.system.service.IDataBaseService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @author dinghao
 * @version 1.0
 * @description
 * @date 2023/6/12 16:33
 */
@Service
public class DataBaseServiceImpl implements IDataBaseService {
    @Value(value = "${icebergManager.hiveConf}")
    private String uploadHiveFilePath;
    @Value(value = "${icebergManager.hadoopUser}")
    private String hadoopUser;

    @Override
    public List<String> databaseList(IcebergCatalogParam icebergCatalogParam) throws Exception {
        String id = icebergCatalogParam.getId();
        String warehouse = icebergCatalogParam.getHdfsurl();
        String types = icebergCatalogParam.getTypes();
        String hiveUrl = icebergCatalogParam.getHiveurl();
        return PDIcebergUtils.build(id,types,hiveUrl,warehouse,hadoopUser,uploadHiveFilePath)
                .listDataBases();
    }

    @Override
    public void addDatabase(IcebergCatalogParam icebergCatalogParam) throws Exception {
        String id = icebergCatalogParam.getId();
        String warehouse = icebergCatalogParam.getHdfsurl();
        String types = icebergCatalogParam.getTypes();
        String hiveUrl = icebergCatalogParam.getHiveurl();
        PDIcebergUtils.build(id,types,hiveUrl,warehouse,hadoopUser,uploadHiveFilePath)
                .addDatabases(icebergCatalogParam.getDatabaseName());
    }

    @Override
    public void deleteDatabase(IcebergCatalogParam icebergCatalogParam) throws Exception {
        String id = icebergCatalogParam.getId();
        String warehouse = icebergCatalogParam.getHdfsurl();
        String types = icebergCatalogParam.getTypes();
        String hiveUrl = icebergCatalogParam.getHiveurl();
        PDIcebergUtils.build(id,types,hiveUrl,warehouse,hadoopUser,uploadHiveFilePath)
                .deleteDatabases(icebergCatalogParam.getDatabaseName());
    }
}
