package com.powerdata.system.service;


import com.powerdata.system.domain.IcebergSaveSql;
import com.powerdata.system.domain.param.DelSaveSqlParam;
import com.powerdata.system.domain.param.IcebergExecLogParam;
import com.powerdata.system.domain.param.IcebergSaveSqlParam;

import java.util.Map;

/**
 * @author dinghao
 * @version 1.0
 * @description
 * @date 2023/8/24 16:09
 */
public interface IcebergSaveSqlService {
    Map<String,Object> sqlList(IcebergSaveSqlParam icebergSaveSqlParam) throws Exception;

    void add(IcebergSaveSql icebergSaveSql) throws Exception;

    void modify(IcebergSaveSql icebergSaveSql) throws Exception;

    void delete(DelSaveSqlParam delSaveSqlParam) throws Exception;

    Map<String,Object> sqlHistoryList(IcebergExecLogParam icebergExecLogParam) throws Exception;
}
