package com.powerdata.system.service.impl;

import com.powerdata.common.utils.iceberg.PDHdfsUtils;
import com.powerdata.system.domain.IcebergCatalog;
import com.powerdata.system.domain.IcebergCatalogExample;
import com.powerdata.system.domain.param.IcebergFileParam;
import com.powerdata.system.mapper.IcebergCatalogMapper;
import com.powerdata.system.service.IUnStructuredService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.ObjectUtils;
import org.springframework.web.multipart.MultipartFile;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author dinghao
 * @version 1.0
 * @description
 * @date 2023/7/20 15:18
 */
@Service
public class UnStructuredServiceImpl implements IUnStructuredService {

    @Value(value = "${icebergManager.hadoopUser}")
    private String hadoopUser;
    @Resource
    private IcebergCatalogMapper icebergCatalogMapper;

    @Override
    public Map<String, Object> listFile(IcebergFileParam icebergFileParam) throws Exception {
        String catalogId = icebergFileParam.getCatalogId();
        String hdfsUrl = icebergFileParam.getHdfsUrl();
        String dirPath = icebergFileParam.getDirPath();
        String fileName = icebergFileParam.getFileName();
        int pageNum = ObjectUtils.isEmpty(icebergFileParam.getPageNum()) ? 0 : icebergFileParam.getPageNum();
        int pageSize = ObjectUtils.isEmpty(icebergFileParam.getPageSize()) ? 0 : icebergFileParam.getPageSize();
        return PDHdfsUtils.build(catalogId,hdfsUrl,hadoopUser).getFileList(dirPath,fileName,pageSize,pageNum);
    }

    @Override
    public Map<String, Object> batchUploadFileToHdfs( String catalogId,String hdfsUrl,
                                                      String dstPath,List<MultipartFile> files) throws Exception {
        return PDHdfsUtils.build(catalogId,hdfsUrl,hadoopUser).batchUploadFileToHdfs(dstPath,files);
    }

    @Override
    public void fastDownloadHdfsFile(HttpServletRequest request, HttpServletResponse response,
                                     String catalogId, String hdfsUrl,
                                     String filePath, String fileName) throws Exception {
        PDHdfsUtils.build(catalogId,hdfsUrl,hadoopUser).downloadHdfsFile(request,response,filePath,fileName);
    }

    @Override
    public void batchDeleteFile(IcebergFileParam icebergFileParam) throws Exception {
        String catalogId = icebergFileParam.getCatalogId();
        String hdfsUrl = icebergFileParam.getHdfsUrl();
        String dirPath = icebergFileParam.getFilePath();
        List<String> fileNames = icebergFileParam.getFileNames();
        PDHdfsUtils.build(catalogId,hdfsUrl,hadoopUser).deleteFiles(dirPath,fileNames);
    }

    @Override
    public void reviewMP4AndPic(HttpServletResponse response,
                                String catalogId, String hdfsUrl, String filePath, String fileName) throws Exception {
        PDHdfsUtils.build(catalogId,hdfsUrl,hadoopUser).reviewMP4AndPic(response,filePath,fileName);
    }

    @Override
    public String catFile(IcebergFileParam icebergFileParam) throws Exception {
        String catalogId = icebergFileParam.getCatalogId();
        String hdfsUrl = icebergFileParam.getHdfsUrl();
        String dirPath = icebergFileParam.getDirPath();
        String fileName = icebergFileParam.getFileName();
        return PDHdfsUtils.build(catalogId,hdfsUrl,hadoopUser).catFile(dirPath,fileName);
    }

    @Override
    public void addDir(IcebergFileParam icebergFileParam) throws Exception {
        String catalogId = icebergFileParam.getCatalogId();
        String hdfsUrl = icebergFileParam.getHdfsUrl();
        String dirPath = icebergFileParam.getDirPath();
        String dirName = icebergFileParam.getDirName();
        PDHdfsUtils.build(catalogId,hdfsUrl,hadoopUser).mkdirDir(dirPath,dirName);
    }

    @Override
    public Map<String, Object> checkAndDeleteFile(IcebergFileParam icebergFileParam) throws Exception {
        HashMap<String, Object> resultMap = new HashMap<>();
        ArrayList<String> successFiles = new ArrayList<>();
        ArrayList<String> errorFiles = new ArrayList<>();
        HashMap<String, String> errorMessage = new HashMap<>();
        String dirPath = icebergFileParam.getFilePath();
        String hdfsUrl = icebergFileParam.getHdfsUrl().endsWith("/")?
                icebergFileParam.getHdfsUrl().substring(0,icebergFileParam.getHdfsUrl().length()-1):
                icebergFileParam.getHdfsUrl();
        List<String> fileNames = icebergFileParam.getFileNames();
        String deletePath = hdfsUrl;
        if(!dirPath.equals("/")){
            deletePath = deletePath+dirPath;
        }

        IcebergCatalogExample icebergCatalogExample = new IcebergCatalogExample();
        icebergCatalogExample.createCriteria().andTypesNotEqualTo("file");
        List<IcebergCatalog> icebergCatalogs = icebergCatalogMapper.selectByExample(icebergCatalogExample);
        HashMap<String, String> deleteFilesMap = new HashMap<>();
        for (String fileName : fileNames) {
            deleteFilesMap.put(fileName,deletePath + "/" + fileName);
        }

        for (Map.Entry<String, String> filesEntry : deleteFilesMap.entrySet()) {
            boolean checkFlag = false;
            for (IcebergCatalog icebergCatalog : icebergCatalogs) {
                if(filesEntry.getValue().startsWith(icebergCatalog.getHdfsurl())||
                        icebergCatalog.getHdfsurl().startsWith(filesEntry.getValue())){
                    errorMessage.put(filesEntry.getKey(),
                            "该文件/目录归属catalog【"+icebergCatalog.getId()+"】的数据目录，删除可能导致该catalog数据异常，确认是否强制删除");
                    errorFiles.add(filesEntry.getKey());
                    checkFlag = true;
                    break;
                }
            }
            if(checkFlag){
                fileNames.remove(filesEntry.getKey());
            }
        }
        batchDeleteFile(icebergFileParam);
        successFiles.addAll(fileNames);
        resultMap.put("successFiles",successFiles);
        resultMap.put("errorFiles",errorFiles);
        resultMap.put("errorMessage",errorMessage);
        return resultMap;
    }

    @Override
    public void fastDownloadHdfsDir(HttpServletRequest request, HttpServletResponse response,
                                    String catalogId, String hdfsUrl, String filePath, String fileName) throws Exception {
        PDHdfsUtils.build(catalogId,hdfsUrl,hadoopUser).downloadDir(request,response,filePath,fileName);
    }

    @Override
    public void moveFile(IcebergFileParam icebergFileParam) throws Exception {
        String catalogId = icebergFileParam.getCatalogId();
        String hdfsUrl = icebergFileParam.getHdfsUrl();
        String filePath = icebergFileParam.getFilePath();
        List<String> fileNames = icebergFileParam.getFileNames();
        String dirPath = icebergFileParam.getDirPath();
        PDHdfsUtils.build(catalogId,hdfsUrl,hadoopUser).moveFile(filePath,fileNames,dirPath);
    }

    @Override
    public void renameFile(IcebergFileParam icebergFileParam) throws Exception {
        String catalogId = icebergFileParam.getCatalogId();
        String hdfsUrl = icebergFileParam.getHdfsUrl();
        String dirPath = icebergFileParam.getDirPath();
        String fileName = icebergFileParam.getFileName();
        String reName = icebergFileParam.getReName();
        PDHdfsUtils.build(catalogId,hdfsUrl,hadoopUser).renameFile(dirPath,fileName,reName);
    }

}
