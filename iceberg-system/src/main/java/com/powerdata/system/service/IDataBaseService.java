package com.powerdata.system.service;

import com.powerdata.system.domain.param.IcebergCatalogParam;

import java.util.List;

/**
 * @author dinghao
 * @version 1.0
 * @description
 * @date 2023/6/12 16:33
 */
public interface IDataBaseService {
    List<String> databaseList(IcebergCatalogParam icebergCatalogParam) throws Exception;

    void addDatabase(IcebergCatalogParam icebergCatalogParam) throws Exception;

    void deleteDatabase(IcebergCatalogParam icebergCatalogParam) throws Exception;
}
