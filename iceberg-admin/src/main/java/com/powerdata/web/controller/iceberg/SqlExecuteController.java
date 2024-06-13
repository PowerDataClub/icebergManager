package com.powerdata.web.controller.iceberg;

import com.powerdata.common.annotation.Log;
import com.powerdata.common.core.domain.AjaxResult;
import com.powerdata.common.enums.BusinessType;
import com.powerdata.system.domain.param.IcebergCatalogParam;
import com.powerdata.system.domain.param.IcebergCopyTableParam;
import com.powerdata.system.service.ISqlExecuteService;
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
 * @date 2023/6/16 15:00
 */
@RestController
@RequestMapping("/iceberg/sql")
@Slf4j
public class SqlExecuteController {
    @Resource
    private ISqlExecuteService iSqlExecuteService;

    @Log(title = "sql执行", businessType = BusinessType.QUERY)
    @ApiOperation("sql执行")
    @PostMapping("/execute")
    public AjaxResult executeSql(@RequestBody IcebergCatalogParam icebergCatalogParam){
        try {
            return AjaxResult.success(iSqlExecuteService.executeSql(icebergCatalogParam));
        }catch (Exception e){
            e.printStackTrace();
            return AjaxResult.error("SQL执行异常："+e.getMessage());
        }
    }

    @Log(title = "表复制", businessType = BusinessType.OTHER)
    @ApiOperation("表复制")
    @PostMapping("/copyTable")
    public AjaxResult copyTable(@RequestBody IcebergCopyTableParam icebergCopyTableParam){
        try {
            iSqlExecuteService.copyTable(icebergCopyTableParam);
            return AjaxResult.success("表复制完成");
        }catch (Exception e){
            e.printStackTrace();
            return AjaxResult.error("表复制失败："+e.getMessage());
        }
    }
}
