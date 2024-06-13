package com.powerdata.system.service;

import com.powerdata.system.domain.IcebergSyncTask;
import com.powerdata.system.domain.param.IcebergSyncTaskParam;

import java.util.Map;

/**
 * @author dinghao
 * @version 1.0
 * @description
 * @date 2023/8/29 16:08
 */
public interface IcebergSyncTaskService {
    Map<String,Object> taskList(IcebergSyncTaskParam icebergSyncTaskParam) throws Exception;

    void add(IcebergSyncTask icebergSyncTask) throws Exception;

    void stopKafkaTask(IcebergSyncTask icebergSyncTask) throws Exception;

    void batchAddHiveTask(IcebergSyncTask icebergSyncTask) throws Exception;

    void batchAddMySqlTask(IcebergSyncTask icebergSyncTask) throws Exception;
}
