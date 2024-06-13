package com.powerdata.web.controller.iceberg;

import com.powerdata.common.annotation.Log;
import com.powerdata.common.core.domain.AjaxResult;
import com.powerdata.common.enums.BusinessType;
import com.powerdata.system.domain.param.IcebergCatalogParam;
import com.powerdata.system.domain.param.IcebergTableParam;
import com.powerdata.system.domain.param.TableToDataParam;
import com.powerdata.system.domain.param.UpdateDataParam;
import com.powerdata.system.service.ITableService;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import javax.annotation.Resource;

/**
 * @author dinghao
 * @version 1.0
 * @description
 * @date 2023/6/12 16:47
 */
@RestController
@RequestMapping("/iceberg/table")
@Slf4j
public class TableController {

    @Resource
    private ITableService iTableService;

    @Log(title = "获取表列表", businessType = BusinessType.QUERY)
    @ApiOperation("获取表列表")
    @PostMapping("/list")
    public AjaxResult tableList(@RequestBody IcebergCatalogParam icebergCatalogParam){
        try {
            return AjaxResult.success(iTableService.tableList(icebergCatalogParam));
        }catch (Exception e){
            e.printStackTrace();
            return AjaxResult.error("获取表列表失败："+e.getMessage());
        }
    }

    @Log(title = "分页获取表列表", businessType = BusinessType.QUERY)
    @ApiOperation("分页获取表列表")
    @PostMapping("/list2")
    public AjaxResult tableList2(@RequestBody IcebergCatalogParam icebergCatalogParam){
        try {
            return AjaxResult.success(iTableService.tableList2(icebergCatalogParam));
        }catch (Exception e){
            e.printStackTrace();
            return AjaxResult.error("获取表列表失败："+e.getMessage());
        }
    }

    @Log(title = "删除表", businessType = BusinessType.DELETE)
    @ApiOperation("删除表")
    @PostMapping("/delete")
    public AjaxResult deleteTable(@RequestBody IcebergCatalogParam icebergCatalogParam){
        try {
            iTableService.deleteTable(icebergCatalogParam);
            return AjaxResult.success("删除成功");
        }catch (Exception e){
            e.printStackTrace();
            return AjaxResult.error("删除表失败："+e.getMessage());
        }
    }

    @Log(title = "表重命名", businessType = BusinessType.UPDATE)
    @ApiOperation("表重命名")
    @PostMapping("/rename")
    public AjaxResult renameTable(@RequestBody IcebergCatalogParam icebergCatalogParam){
        try {
            iTableService.renameTable(icebergCatalogParam);
            return AjaxResult.success("表重命名成功");
        }catch (Exception e){
            e.printStackTrace();
            return AjaxResult.error("表重命名失败："+e.getMessage());
        }
    }

    @Log(title = "获取表元数据文件列表", businessType = BusinessType.QUERY)
    @ApiOperation("获取表元数据列表")
    @PostMapping("/getMetadataFiles")
    public AjaxResult getMetadataFiles(@RequestBody IcebergCatalogParam icebergCatalogParam){
        try {
            return AjaxResult.success(iTableService.getMetadataFiles(icebergCatalogParam));
        }catch (Exception e){
            e.printStackTrace();
            return AjaxResult.error("获取表元数据列表失败："+e.getMessage());
        }
    }

    @Log(title = "查看元数据文件内容", businessType = BusinessType.QUERY)
    @ApiOperation("查看元数据文件内容")
    @PostMapping("/getMetadata")
    public AjaxResult getMetadata(@RequestBody IcebergCatalogParam icebergCatalogParam){
        try {
            return AjaxResult.success("查看元数据文件内容成功",iTableService.getMetadata(icebergCatalogParam));
        }catch (Exception e){
            e.printStackTrace();
            return AjaxResult.error("查看元数据文件内容失败："+e.getMessage());
        }
    }

    @Log(title = "表移动库", businessType = BusinessType.UPDATE)
    @ApiOperation("表移动库")
    @PostMapping("/move")
    public AjaxResult moveTable(@RequestBody IcebergCatalogParam icebergCatalogParam){
        try {
            iTableService.moveTable(icebergCatalogParam);
            return AjaxResult.success("表移动库成功");
        }catch (Exception e){
            e.printStackTrace();
            return AjaxResult.error("表移动库失败："+e.getMessage());
        }
    }

    @Log(title = "获取表统计信息", businessType = BusinessType.QUERY)
    @ApiOperation("获取表统计信息")
    @PostMapping("/metrics")
    public AjaxResult getTableMetrics(@RequestBody IcebergCatalogParam icebergCatalogParam){
        try {
            return AjaxResult.success(iTableService.getTableMetrics(icebergCatalogParam));
        }catch (Exception e){
            e.printStackTrace();
            return AjaxResult.error("获取表统计信息失败："+e.getMessage());
        }
    }

    @Log(title = "获取表字段信息", businessType = BusinessType.QUERY)
    @ApiOperation("获取表字段信息")
    @PostMapping("/columns")
    public AjaxResult getColumnsOfTable(@RequestBody IcebergCatalogParam icebergCatalogParam){
        try {
            return AjaxResult.success(iTableService.getColumnsOfTable(icebergCatalogParam));
        }catch (Exception e){
            e.printStackTrace();
            return AjaxResult.error("获取表字段信息失败："+e.getMessage());
        }
    }

    @Log(title = "获取表分区信息", businessType = BusinessType.QUERY)
    @ApiOperation("获取表分区信息")
    @PostMapping("/partition")
    public AjaxResult getPartitionMessage(@RequestBody IcebergCatalogParam icebergCatalogParam){
        try {
            return AjaxResult.success(iTableService.getPartitionMessage(icebergCatalogParam));
        }catch (Exception e){
            e.printStackTrace();
            return AjaxResult.error("获取表分区信息失败："+e.getMessage());
        }
    }

    @Log(title = "获取表快照列表信息", businessType = BusinessType.QUERY)
    @ApiOperation("获取表快照列表信息")
    @PostMapping("/snapshot")
    public AjaxResult getTransactionsMessage(@RequestBody IcebergCatalogParam icebergCatalogParam){
        try {
            return AjaxResult.success(iTableService.getTransactionsMessage(icebergCatalogParam));
        }catch (Exception e){
            e.printStackTrace();
            return AjaxResult.error("获取表快照信息失败："+e.getMessage());
        }
    }

    @Log(title = "获取表数据", businessType = BusinessType.QUERY)
    @ApiOperation("获取表数据")
    @PostMapping("/getData")
    public AjaxResult getTableData(@RequestBody IcebergCatalogParam icebergCatalogParam){
        try {
            return AjaxResult.success(iTableService.getTableData(icebergCatalogParam));
        }catch (Exception e){
            e.printStackTrace();
            return AjaxResult.error("获取表数据信息失败："+e.getMessage());
        }
    }

    @Log(title = "获取指定快照版本的表数据", businessType = BusinessType.QUERY)
    @ApiOperation("获取指定快照版本的表数据")
    @PostMapping("/getDataBySnapshotId")
    public AjaxResult getDataBySnapshotId(@RequestBody IcebergCatalogParam icebergCatalogParam){
        try {
            return AjaxResult.success(iTableService.getDataBySnapshotId(icebergCatalogParam));
        }catch (Exception e){
            e.printStackTrace();
            return AjaxResult.error("获取表数据信息失败："+e.getMessage());
        }
    }

    @Log(title = "获取指定时间之前的表数据", businessType = BusinessType.QUERY)
    @ApiOperation("获取指定时间之前的表数据")
    @PostMapping("/getDataByTime")
    public AjaxResult getDataByTime(@RequestBody IcebergCatalogParam icebergCatalogParam){
        try {
            return AjaxResult.success(iTableService.getDataByTime(icebergCatalogParam));
        }catch (Exception e){
            e.printStackTrace();
            if(e.getMessage().contains("No value present")){
                return AjaxResult.error("获取表数据信息失败：指定时间前无数据存在，请重新指定时空查询的时间");
            }
            return AjaxResult.error("获取表数据信息失败："+e.getMessage());
        }
    }

    @Log(title = "切换快照", businessType = BusinessType.UPDATE)
    @ApiOperation("切换快照")
    @PostMapping("/setCurrentSnapshot")
    public AjaxResult setCurrentSnapshot(@RequestBody IcebergCatalogParam icebergCatalogParam){
        try {
            iTableService.setCurrentSnapshot(icebergCatalogParam);
            return AjaxResult.success("快照切换成功");
        }catch (Exception e){
            e.printStackTrace();
            return AjaxResult.error("切换快照失败："+e.getMessage());
        }
    }

    @Log(title = "回滚快照", businessType = BusinessType.UPDATE)
    @ApiOperation("回滚快照")
    @PostMapping("/rollbackSnapshot")
    public AjaxResult rollbackSnapshot(@RequestBody IcebergCatalogParam icebergCatalogParam){
        try {
            iTableService.rollbackSnapshot(icebergCatalogParam);
            return AjaxResult.success("快照回滚成功");
        }catch (Exception e){
            e.printStackTrace();
            return AjaxResult.error("快照回滚失败："+e.getMessage());
        }
    }

    @Log(title = "cherry pick快照", businessType = BusinessType.UPDATE)
    @ApiOperation("cherry pick快照")
    @PostMapping("/cherryPickSnapshot")
    public AjaxResult cherryPickSnapshot(@RequestBody IcebergCatalogParam icebergCatalogParam){
        try {
            iTableService.cherryPickSnapshot(icebergCatalogParam);
            return AjaxResult.success("快照选择合并成功");
        }catch (Exception e){
            e.printStackTrace();
            return AjaxResult.error("快照选择合并失败："+e.getMessage());
        }
    }

    @Log(title = "合并小文件", businessType = BusinessType.DELETE)
    @ApiOperation("合并小文件")
    @PostMapping("/mergeSmallFile")
    public AjaxResult mergeSmallFile(@RequestBody IcebergCatalogParam icebergCatalogParam){
        try {
            iTableService.mergeSmallFile(icebergCatalogParam);
            return AjaxResult.success("合并小文件执行完成");
        }catch (Exception e){
            e.printStackTrace();
            return AjaxResult.error("合并小文件执行失败："+e.getMessage());
        }
    }

    @Log(title = "获取某快照后新增数据", businessType = BusinessType.QUERY)
    @ApiOperation("获取某快照后新增数据")
    @PostMapping("/getAppendSnapshotTableData")
    public AjaxResult getAppendSnapshotTableData(@RequestBody IcebergCatalogParam icebergCatalogParam){
        try {
            return AjaxResult.success(iTableService.getAppendSnapshotTableData(icebergCatalogParam));
        }catch (Exception e){
            e.printStackTrace();
            return AjaxResult.error("获取某快照后新增数据失败："+e.getMessage());
        }
    }

    @Log(title = "获取快照之间的新增数据", businessType = BusinessType.QUERY)
    @ApiOperation("获取快照之间的新增数据")
    @PostMapping("/getBetweenSnapshotTableData")
    public AjaxResult getBetweenSnapshotTableData(@RequestBody IcebergCatalogParam icebergCatalogParam){
        try {
            return AjaxResult.success(iTableService.getBetweenSnapshotTableData(icebergCatalogParam));
        }catch (Exception e){
            e.printStackTrace();
            return AjaxResult.error("获取快照之间的新增数据失败："+e.getMessage());
        }
    }

    @Log(title = "新增表数据", businessType = BusinessType.INSERT)
    @ApiOperation("新增表数据")
    @PostMapping("/addData")
    public AjaxResult addData(@RequestBody IcebergCatalogParam icebergCatalogParam){
        try {
            iTableService.addData(icebergCatalogParam);
            return AjaxResult.success("新增表数据成功");
        }catch (Exception e){
            e.printStackTrace();
            return AjaxResult.error("新增表数据失败："+e.getMessage());
        }
    }

    @Log(title = "删除表数据", businessType = BusinessType.DELETE)
    @ApiOperation("删除表数据")
    @PostMapping("/deleteData")
    public AjaxResult deleteData(@RequestBody IcebergCatalogParam icebergCatalogParam){
        try {
            iTableService.deleteData(icebergCatalogParam);
            return AjaxResult.success("删除表数据成功");
        }catch (Exception e){
            e.printStackTrace();
            return AjaxResult.error("删除表数据失败："+e.getMessage());
        }
    }

    @Log(title = "更新表数据", businessType = BusinessType.UPDATE)
    @ApiOperation("更新表数据")
    @PostMapping("/updateData")
    public AjaxResult updateData(@RequestBody UpdateDataParam updateDataParam){
        try {
            iTableService.updateData(updateDataParam);
            return AjaxResult.success("更新表数据成功");
        }catch (Exception e){
            e.printStackTrace();
            return AjaxResult.error("更新表数据失败："+e.getMessage());
        }
    }

    @Log(title = "创建表", businessType = BusinessType.INSERT)
    @ApiOperation("创建表")
    @PostMapping("/createTable")
    public AjaxResult createTable(@RequestBody IcebergTableParam icebergTableParam){
        try {
            iTableService.createTable(icebergTableParam);
            return AjaxResult.success("创建表成功");
        }catch (Exception e){
            e.printStackTrace();
            return AjaxResult.error("创建表失败："+e.getMessage());
        }
    }

    @Log(title = "修改表", businessType = BusinessType.UPDATE)
    @ApiOperation("修改表")
    @PostMapping("/updateTable")
    public AjaxResult updateTable(@RequestBody IcebergTableParam icebergTableParam){
        try {
            iTableService.updateTable(icebergTableParam);
            return AjaxResult.success("修改表成功");
        }catch (Exception e){
            e.printStackTrace();
            return AjaxResult.error("修改表失败："+e.getMessage());
        }
    }

    @Log(title = "查询编辑表信息", businessType = BusinessType.QUERY)
    @ApiOperation("查询编辑表信息")
    @PostMapping("/queryTableInfo")
    public AjaxResult queryTableInfo(@RequestBody IcebergTableParam icebergTableParam){
        try {
            return AjaxResult.success(iTableService.queryTableInfo(icebergTableParam));
        }catch (Exception e){
            log.error("查询编辑表信息失败",e);
            return AjaxResult.error("查询编辑表信息失败："+e.getMessage());
        }
    }

    @Log(title = "Excel数据导入", businessType = BusinessType.IMPORT)
    @ApiOperation("Excel数据导入")
    @PostMapping("/importData")
    public AjaxResult importDataToTable(@RequestParam String id,@RequestParam String types, @RequestParam String hiveurl,
                                        @RequestParam String hdfsurl,@RequestParam String databaseName,
                                        @RequestParam String tableName,@RequestParam("file") MultipartFile file){
        try {
            iTableService.importDataToTable(id,types,hiveurl,hdfsurl,databaseName,tableName,file);
            return AjaxResult.success("Excel数据导入成功");
        }catch (Exception e){
            e.printStackTrace();
            return AjaxResult.error("Excel数据导入失败："+e.getMessage());
        }
    }

    @Log(title = "表数据转化为文件", businessType = BusinessType.IMPORT)
    @ApiOperation("表数据转化为文件")
    @PostMapping("/tableDataToFile")
    public AjaxResult tableDataToFile(@RequestBody TableToDataParam tableToDataParam){
        try {
            iTableService.tableDataToFile(tableToDataParam);
            return AjaxResult.success("表数据转化为文件完成");
        }catch (Exception e){
            e.printStackTrace();
            return AjaxResult.error("表数据转化为文件失败："+e.getMessage());
        }
    }

    @Log(title = "非结构化文件加载到表", businessType = BusinessType.IMPORT)
    @ApiOperation("非结构化文件加载到表")
    @PostMapping("/fileToTableData")
    public AjaxResult fileToTableData(@RequestBody TableToDataParam tableToDataParam){
        try {
            iTableService.fileToTableData(tableToDataParam);
            return AjaxResult.success("非结构化文件加载到表完成");
        }catch (Exception e){
            e.printStackTrace();
            return AjaxResult.error("非结构化文件加载到表失败："+e.getMessage());
        }
    }

    @Log(title = "清空表", businessType = BusinessType.UPDATE)
    @ApiOperation("清空表")
    @PostMapping("/clearTableData")
    public AjaxResult clearTableData(@RequestBody IcebergTableParam icebergTableParam){
        try {
            iTableService.clearTableData(icebergTableParam);
            return AjaxResult.success("清空表成功");
        }catch (Exception e){
            e.printStackTrace();
            return AjaxResult.error("清空表失败："+e.getMessage());
        }
    }

}
