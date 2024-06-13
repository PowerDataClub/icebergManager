package com.powerdata.web.controller.iceberg;

import com.powerdata.common.annotation.Log;
import com.powerdata.common.core.domain.AjaxResult;
import com.powerdata.common.enums.BusinessType;
import com.powerdata.system.domain.IcebergSyncTask;
import com.powerdata.system.domain.param.IcebergSyncTaskParam;
import com.powerdata.system.service.IcebergSyncTaskService;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;

/**
 * @author dinghao
 * @version 1.0
 * @description
 * @date 2023/8/29 16:10
 */
@RestController
@RequestMapping("/iceberg/syncTask")
@Slf4j
public class SyncTaskController {

    @Resource
    private IcebergSyncTaskService icebergSyncTaskService;

    @Log(title = "获取入湖任务列表", businessType = BusinessType.QUERY)
    @ApiOperation("获取入湖任务列表")
    @PostMapping("/list")
    public AjaxResult taskList(@RequestBody IcebergSyncTaskParam icebergSyncTaskParam){
        try {
            return AjaxResult.success(icebergSyncTaskService.taskList(icebergSyncTaskParam));
        }catch (Exception e){
            e.printStackTrace();
            return AjaxResult.error("获取入湖任务列表失败："+e.getMessage());
        }
    }

    @Log(title = "创建并执行入湖任务", businessType = BusinessType.INSERT)
    @ApiOperation("创建并执行入湖任务")
    @PostMapping("/add")
    public AjaxResult add(@RequestBody IcebergSyncTask icebergSyncTask){
        try {
            icebergSyncTaskService.add(icebergSyncTask);
            return AjaxResult.success("新增成功");
        }catch (Exception e){
            e.printStackTrace();
            return AjaxResult.error("创建并执行入湖任务失败："+e.getMessage());
        }
    }

    @Log(title = "修改并执行入湖任务", businessType = BusinessType.UPDATE)
    @ApiOperation("修改并执行入湖任务")
    @PostMapping("/modify")
    public AjaxResult modify(@RequestBody IcebergSyncTask IcebergSyncTask){
        try {
            icebergSyncTaskService.add(IcebergSyncTask);
            return AjaxResult.success("修改成功");
        }catch (Exception e){
            e.printStackTrace();
            return AjaxResult.error("修改并执行入湖任务失败："+e.getMessage());
        }
    }

    @Log(title = "停止kafka的实时入湖任务", businessType = BusinessType.UPDATE)
    @ApiOperation("停止kafka的实时入湖任务")
    @PostMapping("/stopKafkaTask")
    public AjaxResult stopKafkaTask(@RequestBody IcebergSyncTask IcebergSyncTask){
        try {
            icebergSyncTaskService.stopKafkaTask(IcebergSyncTask);
            return AjaxResult.success("停止成功");
        }catch (Exception e){
            e.printStackTrace();
            return AjaxResult.error("停止kafka的实时入湖任务失败："+e.getMessage());
        }
    }

    @Log(title = "批量创建并执行Hive入湖任务", businessType = BusinessType.INSERT)
    @ApiOperation("批量创建并执行Hive入湖任务")
    @PostMapping("/batchAddHiveTask")
    public AjaxResult batchAddHiveTask(@RequestBody IcebergSyncTask icebergSyncTask){
        try {
            icebergSyncTaskService.batchAddHiveTask(icebergSyncTask);
            return AjaxResult.success("批量创建并执行Hive入湖任务成功");
        }catch (Exception e){
            e.printStackTrace();
            return AjaxResult.error("批量创建并执行Hive入湖任务失败："+e.getMessage());
        }
    }

    @Log(title = "批量创建并执行mysql入湖任务", businessType = BusinessType.INSERT)
    @ApiOperation("批量创建并执行mysql入湖任务")
    @PostMapping("/batchAddMySqlTask")
    public AjaxResult batchAddMySqlTask(@RequestBody IcebergSyncTask icebergSyncTask){
        try {
            icebergSyncTaskService.batchAddMySqlTask(icebergSyncTask);
            return AjaxResult.success("批量创建并执行mysql入湖任务成功");
        }catch (Exception e){
            e.printStackTrace();
            return AjaxResult.error("批量创建并执行mysql入湖任务失败："+e.getMessage());
        }
    }
}
