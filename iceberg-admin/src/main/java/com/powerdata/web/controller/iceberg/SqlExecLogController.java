package com.powerdata.web.controller.iceberg;

import com.powerdata.common.annotation.Log;
import com.powerdata.common.core.domain.AjaxResult;
import com.powerdata.common.enums.BusinessType;
import com.powerdata.system.domain.IcebergSaveSql;
import com.powerdata.system.domain.param.DelSaveSqlParam;
import com.powerdata.system.domain.param.IcebergExecLogParam;
import com.powerdata.system.domain.param.IcebergSaveSqlParam;
import com.powerdata.system.service.IcebergSaveSqlService;
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
 * @date 2023/8/24 16:11
 */
@RestController
@RequestMapping("/iceberg/sqlLog")
@Slf4j
public class SqlExecLogController {
    @Resource
    private IcebergSaveSqlService icebergSaveSqlService;

    @Log(title = "获取保存的sql列表", businessType = BusinessType.QUERY)
    @ApiOperation("获取保存的sql列表")
    @PostMapping("/list")
    public AjaxResult sqlList(@RequestBody IcebergSaveSqlParam icebergSaveSqlParam){
        try {
            return AjaxResult.success(icebergSaveSqlService.sqlList(icebergSaveSqlParam));
        }catch (Exception e){
            e.printStackTrace();
            return AjaxResult.error("获取保存的sql列表失败："+e.getMessage());
        }
    }

    @Log(title = "新增保存的sql", businessType = BusinessType.INSERT)
    @ApiOperation("新增保存的sql")
    @PostMapping("/add")
    public AjaxResult add(@RequestBody IcebergSaveSql icebergSaveSql){
        try {
            icebergSaveSqlService.add(icebergSaveSql);
            return AjaxResult.success("新增成功");
        }catch (Exception e){
            e.printStackTrace();
            return AjaxResult.error("新增保存的sql失败："+e.getMessage());
        }
    }

    @Log(title = "修改保存的sql", businessType = BusinessType.UPDATE)
    @ApiOperation("修改保存的sql")
    @PostMapping("/modify")
    public AjaxResult modify(@RequestBody IcebergSaveSql icebergSaveSql){
        try {
            icebergSaveSqlService.modify(icebergSaveSql);
            return AjaxResult.success("修改成功");
        }catch (Exception e){
            e.printStackTrace();
            return AjaxResult.error("修改保存的sql失败："+e.getMessage());
        }
    }

    @Log(title = "删除保存的sql", businessType = BusinessType.DELETE)
    @ApiOperation("删除保存的sql")
    @PostMapping("/delete")
    public AjaxResult delete(@RequestBody DelSaveSqlParam delSaveSqlParam){
        try {
            icebergSaveSqlService.delete(delSaveSqlParam);
            return AjaxResult.success("删除成功");
        }catch (Exception e){
            e.printStackTrace();
            return AjaxResult.error("删除保存的sql失败："+e.getMessage());
        }
    }

    @Log(title = "查询执行的sql历史列表", businessType = BusinessType.QUERY)
    @ApiOperation("查询执行的sql历史列表")
    @PostMapping("/history")
    public AjaxResult sqlHistoryList(@RequestBody IcebergExecLogParam icebergExecLogParam){
        try {
            return AjaxResult.success(icebergSaveSqlService.sqlHistoryList(icebergExecLogParam));
        }catch (Exception e){
            e.printStackTrace();
            return AjaxResult.error("查询执行的sql历史列表失败："+e.getMessage());
        }
    }
}
