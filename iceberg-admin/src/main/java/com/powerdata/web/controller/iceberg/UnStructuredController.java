package com.powerdata.web.controller.iceberg;

import com.powerdata.common.annotation.Log;
import com.powerdata.common.core.domain.AjaxResult;
import com.powerdata.common.enums.BusinessType;
import com.powerdata.system.domain.param.IcebergFileParam;
import com.powerdata.system.service.IUnStructuredService;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.List;

/**
 * @author dinghao
 * @version 1.0
 * @description
 * @date 2023/7/20 15:15
 */
@RestController
@RequestMapping("/iceberg/unStructured")
@Slf4j
public class UnStructuredController {

    @Resource
    private IUnStructuredService iUnStructuredService;

    @Log(title = "获取非结构化文件列表", businessType = BusinessType.QUERY)
    @ApiOperation("获取非结构化文件列表")
    @PostMapping("/list")
    public AjaxResult listFile(@RequestBody IcebergFileParam icebergFileParam) {
        try {
            return AjaxResult.success(iUnStructuredService.listFile(icebergFileParam));
        } catch (Exception e) {
            e.printStackTrace();
            return AjaxResult.error("获取非结构化文件列表失败：" + e.getMessage());
        }
    }

    @Log(title = "hdfs文件批量上传", businessType = BusinessType.IMPORT)
    @PostMapping("/batchUploadFileToHdfs")
    @ResponseBody
    public AjaxResult batchUploadFileToHdfs(@RequestParam List<MultipartFile> files, String hdfsUrl,String dirPath, String catalogId) throws Exception {
        try {
            return AjaxResult.success(iUnStructuredService.batchUploadFileToHdfs(catalogId,hdfsUrl,dirPath,files));
        } catch (Exception e) {
            e.printStackTrace();
            return AjaxResult.error("hdfs文件批量上传异常：" + e.getMessage());
        }
    }

    @Log(title = "hdfs文件下载", businessType = BusinessType.EXPORT)
    @GetMapping("/fastDownloadHdfsFile/{catalogId}")
    @ResponseBody
    public AjaxResult fastDownloadHdfsFile(HttpServletRequest request, HttpServletResponse response,
                                           @PathVariable String catalogId, String hdfsUrl,
                                           String filePath, String fileName) throws Exception {
        try {
            iUnStructuredService.fastDownloadHdfsFile(request,response,catalogId,hdfsUrl,filePath,fileName);
            return AjaxResult.success("hdfs文件下载成功");
        } catch (Exception e) {
            e.printStackTrace();
            return AjaxResult.error("hdfs文件下载失败：" + e.getMessage());
        }
    }

    @Log(title = "hdfs文件目录下载", businessType = BusinessType.EXPORT)
    @GetMapping("/fastDownloadHdfsDir/{catalogId}")
    @ResponseBody
    public AjaxResult fastDownloadHdfsDir(HttpServletRequest request, HttpServletResponse response,
                                           @PathVariable String catalogId, String hdfsUrl,
                                           String filePath, String fileName) throws Exception {
        try {
            iUnStructuredService.fastDownloadHdfsDir(request,response,catalogId,hdfsUrl,filePath,fileName);
            return AjaxResult.success("hdfs目录下载成功");
        } catch (Exception e) {
            e.printStackTrace();
            return AjaxResult.error("hdfs目录下载失败：" + e.getMessage());
        }
    }

    @Log(title = "hdfs文件目录强制删除", businessType = BusinessType.DELETE)
    @PostMapping("/batchDeleteFile")
    @ResponseBody
    public AjaxResult batchDeleteFile(@RequestBody IcebergFileParam icebergFileParam) throws Exception {
        try {
            iUnStructuredService.batchDeleteFile(icebergFileParam);
            return AjaxResult.success("hdfs文件删除完成");
        } catch (Exception e) {
            e.printStackTrace();
            return AjaxResult.error("hdfs文件批量上传异常：" + e.getMessage());
        }
    }

    @Log(title = "hdfs图片、视频预览", businessType = BusinessType.EXPORT)
    @GetMapping(value = "/reviewMP4AndPic/{catalogId}")
    @ResponseBody
    public AjaxResult reviewMP4AndPic(HttpServletResponse response, @PathVariable String catalogId,String hdfsUrl, String filePath, String fileName) {
        try {
            iUnStructuredService.reviewMP4AndPic(response, catalogId, hdfsUrl, filePath, fileName);
            return AjaxResult.success("图片、视频获取成功");
        } catch (Exception e) {
            return AjaxResult.error("图片、视频预览失败：" + e.getMessage());
        }
    }

    @Log(title = "查看非结构化文件内容", businessType = BusinessType.QUERY)
    @ApiOperation("查看非结构化文件内容")
    @PostMapping("/catFile")
    public AjaxResult catFile(@RequestBody IcebergFileParam icebergFileParam){
        try {
            return AjaxResult.success("查看非结构化文件内容成功",iUnStructuredService.catFile(icebergFileParam));
        }catch (Exception e){
            e.printStackTrace();
            return AjaxResult.error("查看非结构化文件内容失败："+e.getMessage());
        }
    }

    @Log(title = "创建目录", businessType = BusinessType.QUERY)
    @ApiOperation("创建目录")
    @PostMapping("/addDir")
    public AjaxResult addDir(@RequestBody IcebergFileParam icebergFileParam) {
        try {
            iUnStructuredService.addDir(icebergFileParam);
            return AjaxResult.success("目录创建成功");
        } catch (Exception e) {
            e.printStackTrace();
            return AjaxResult.error("创建目录失败：" + e.getMessage());
        }
    }

    @Log(title = "文件/目录批量删除", businessType = BusinessType.QUERY)
    @ApiOperation("文件/目录批量删除")
    @PostMapping("/checkAndDeleteFile")
    public AjaxResult checkAndDeleteFile(@RequestBody IcebergFileParam icebergFileParam) {
        try {
            return AjaxResult.success(iUnStructuredService.checkAndDeleteFile(icebergFileParam));
        } catch (Exception e) {
            e.printStackTrace();
            return AjaxResult.error("文件/目录批量删除异常：" + e.getMessage());
        }
    }

    @Log(title = "文件/目录剪切粘贴", businessType = BusinessType.UPDATE)
    @ApiOperation("文件/目录剪切粘贴")
    @PostMapping("/moveFile")
    public AjaxResult moveFile(@RequestBody IcebergFileParam icebergFileParam) {
        try {
            iUnStructuredService.moveFile(icebergFileParam);
            return AjaxResult.success("文件/目录剪切粘贴成功");
        } catch (Exception e) {
            e.printStackTrace();
            return AjaxResult.error("文件/目录剪切粘贴失败：" + e.getMessage());
        }
    }

    @Log(title = "文件/目录重命名", businessType = BusinessType.UPDATE)
    @ApiOperation("文件/目录重命名")
    @PostMapping("/renameFile")
    public AjaxResult renameFile(@RequestBody IcebergFileParam icebergFileParam) {
        try {
            iUnStructuredService.renameFile(icebergFileParam);
            return AjaxResult.success("文件/目录重命名成功");
        } catch (Exception e) {
            e.printStackTrace();
            return AjaxResult.error("文件/目录重命名失败：" + e.getMessage());
        }
    }
}
