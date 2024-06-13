package com.powerdata.web.controller.iceberg;

import com.powerdata.common.annotation.Log;
import com.powerdata.common.core.domain.AjaxResult;
import com.powerdata.common.enums.BusinessType;
import com.powerdata.system.domain.param.IcebergCatalogParam;
import com.powerdata.system.service.IDataBaseService;
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
 * @date 2023/6/12 16:30
 */
@RestController
@RequestMapping("/iceberg/database")
@Slf4j
public class DataBaseController {
    @Resource
    private IDataBaseService iDataBaseService;

    @Log(title = "获取库列表", businessType = BusinessType.QUERY)
    @ApiOperation("获取库列表")
    @PostMapping("/list")
    public AjaxResult databaseList(@RequestBody IcebergCatalogParam icebergCatalogParam){
        try {
            return AjaxResult.success(iDataBaseService.databaseList(icebergCatalogParam));
        }catch (Exception e){
            e.printStackTrace();
            return AjaxResult.error("获取库列表失败："+e.getMessage());
        }
    }

    @Log(title = "新增库", businessType = BusinessType.INSERT)
    @ApiOperation("新增库")
    @PostMapping("/add")
    public AjaxResult addDatabase(@RequestBody IcebergCatalogParam icebergCatalogParam){
        try {
            iDataBaseService.addDatabase(icebergCatalogParam);
            return AjaxResult.success("新增成功");
        }catch (Exception e){
            e.printStackTrace();
            return AjaxResult.error("新增库失败："+e.getMessage());
        }
    }

    @Log(title = "删除库", businessType = BusinessType.DELETE)
    @ApiOperation("删除库")
    @PostMapping("/delete")
    public AjaxResult deleteDatabase(@RequestBody IcebergCatalogParam icebergCatalogParam){
        try {
            iDataBaseService.deleteDatabase(icebergCatalogParam);
            return AjaxResult.success("删除成功");
        }catch (Exception e){
            e.printStackTrace();
            return AjaxResult.error("删除库失败："+e.getMessage());
        }
    }
}
