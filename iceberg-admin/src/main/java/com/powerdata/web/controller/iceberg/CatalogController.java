package com.powerdata.web.controller.iceberg;

import com.powerdata.common.annotation.Log;
import com.powerdata.common.core.domain.AjaxResult;
import com.powerdata.common.enums.BusinessType;
import com.powerdata.system.domain.IcebergCatalog;
import com.powerdata.system.service.ICatalogService;
import com.powerdata.system.domain.param.IcebergCatalogParam;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * @author dinghao
 * @version 1.0
 * @description
 * @date 2023/6/12 11:08
 */
@RestController
@RequestMapping("/iceberg/catalog")
@Slf4j
public class CatalogController {
    @Resource
    private ICatalogService iCatalogService;

    @PostConstruct
    public void init(){
        iCatalogService.initCatalogUtils();
    }

    @Log(title = "获取catalog列表", businessType = BusinessType.QUERY)
    @ApiOperation("获取catalog列表")
    @PostMapping("/list")
    public AjaxResult catalogList(@RequestBody IcebergCatalogParam icebergCatalogParam){
        try {
            return AjaxResult.success(iCatalogService.catalogList(icebergCatalogParam));
        }catch (Exception e){
            e.printStackTrace();
            return AjaxResult.error("获取catalog列表失败："+e.getMessage());
        }
    }

    @Log(title = "新增catalog", businessType = BusinessType.INSERT)
    @ApiOperation("新增catalog")
    @PostMapping("/add")
    public AjaxResult addCatalog(@RequestBody IcebergCatalog icebergCatalog){
        try {
            iCatalogService.addCatalog(icebergCatalog);
            return AjaxResult.success("新增成功");
        }catch (Exception e){
            e.printStackTrace();
            return AjaxResult.error("新增catalog失败："+e.getMessage());
        }
    }

    @Log(title = "修改catalog", businessType = BusinessType.UPDATE)
    @ApiOperation("修改catalog")
    @PostMapping("/modify")
    public AjaxResult modifyCatalog(@RequestBody IcebergCatalog icebergCatalog){
        try {
            iCatalogService.modifyCatalog(icebergCatalog);
            return AjaxResult.success("修改成功");
        }catch (Exception e){
            e.printStackTrace();
            return AjaxResult.error("修改catalog失败："+e.getMessage());
        }
    }

    @Log(title = "删除catalog", businessType = BusinessType.DELETE)
    @ApiOperation("删除catalog")
    @PostMapping("/delete")
    public AjaxResult deleteCatalog(@RequestBody IcebergCatalog icebergCatalog){
        try {
            iCatalogService.deleteCatalog(icebergCatalog);
            return AjaxResult.success("删除成功");
        }catch (Exception e){
            e.printStackTrace();
            return AjaxResult.error("删除catalog失败："+e.getMessage());
        }
    }

    @ApiOperation("上传hiveCatalog配置文件")
    @PostMapping("/upLoadHiveCatalogFile/{catalogId}")
    @ResponseBody
    public AjaxResult upLoadHiveCatalogFile(@RequestParam("file") MultipartFile file, @PathVariable String catalogId) {
        try {
            return AjaxResult.success(iCatalogService.upLoadHiveCatalogFile(file,catalogId));
        }catch (Exception e){
            e.printStackTrace();
            return AjaxResult.error("上传hiveCatalog配置文件失败："+e.getMessage());
        }
    }

    @ApiOperation("查看hiveCatalog配置文件")
    @GetMapping("/getHiveCatalogFiles/{catalogId}")
    @ResponseBody
    public AjaxResult getHiveCatalogFiles(@PathVariable String catalogId) {
        try {
            return AjaxResult.success(iCatalogService.getHiveCatalogFiles(catalogId));
        }catch (Exception e){
            e.printStackTrace();
            return AjaxResult.error("查看hiveCatalog配置文件失败："+e.getMessage());
        }
    }

    @ApiOperation("删除hiveCatalog配置文件")
    @PostMapping("/deleteHiveCatalogFiles/{catalogId}/{types}")
    @ResponseBody
    public AjaxResult deleteHiveCatalogFiles(@PathVariable String catalogId,@PathVariable String types) {
        try {
            return AjaxResult.success(iCatalogService.deleteHiveCatalogFiles(catalogId,types));
        }catch (Exception e){
            e.printStackTrace();
            return AjaxResult.error("删除hiveCatalog配置文件失败："+e.getMessage());
        }
    }

    @Log(title = "配置文件下载", businessType = BusinessType.OTHER)
    @GetMapping("/downloadFile/{catalogId}/{types}")
    @ResponseBody
    public AjaxResult downloadFile(HttpServletRequest request, HttpServletResponse response,
                                           @PathVariable String catalogId, @PathVariable String types) {
        try {
            iCatalogService.downloadHiveCatalogFile(request,response,catalogId,types);
            return AjaxResult.success("配置文件下载成功");
        } catch (Exception e) {
            e.printStackTrace();
            return AjaxResult.error("配置文件下载失败：" + e.getMessage());
        }
    }
}
