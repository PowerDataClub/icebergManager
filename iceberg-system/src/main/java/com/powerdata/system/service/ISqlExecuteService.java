package com.powerdata.system.service;

import com.powerdata.system.domain.param.IcebergCatalogParam;
import com.powerdata.system.domain.param.IcebergCopyTableParam;

import java.util.Map;

/**
 * @author dinghao
 * @version 1.0
 * @description
 * @date 2023/6/16 15:03
 */
public interface ISqlExecuteService {
    Map<String,Object> executeSql(IcebergCatalogParam icebergCatalogParam) throws Exception;

    void copyTable(IcebergCopyTableParam icebergCopyTableParam) throws Exception;
}
