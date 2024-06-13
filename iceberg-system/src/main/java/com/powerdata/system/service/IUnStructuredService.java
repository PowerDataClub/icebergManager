package com.powerdata.system.service;

import com.powerdata.system.domain.param.IcebergFileParam;
import org.springframework.web.multipart.MultipartFile;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.List;
import java.util.Map;

/**
 * @author dinghao
 * @version 1.0
 * @description
 * @date 2023/7/20 15:18
 */
public interface IUnStructuredService {
    Map<String,Object> listFile(IcebergFileParam icebergFileParam) throws Exception;

    Map<String,Object> batchUploadFileToHdfs(String catalogId, String hdfsUrl,String dstPath,
                                             List<MultipartFile> files) throws Exception;

    void fastDownloadHdfsFile(HttpServletRequest request, HttpServletResponse response,
                              String catalogId, String hdfsUrl, String filePath, String fileName) throws Exception;

    void batchDeleteFile(IcebergFileParam icebergFileParam) throws Exception;

    void reviewMP4AndPic(HttpServletResponse response,
                         String catalogId, String hdfsUrl, String filePath, String fileName) throws Exception;

    String catFile(IcebergFileParam icebergFileParam) throws Exception;

    void addDir(IcebergFileParam icebergFileParam) throws Exception;

    Map<String, Object> checkAndDeleteFile(IcebergFileParam icebergFileParam) throws Exception;

    void fastDownloadHdfsDir(HttpServletRequest request, HttpServletResponse response,
                             String catalogId, String hdfsUrl, String filePath, String fileName) throws Exception;

    void moveFile(IcebergFileParam icebergFileParam) throws Exception;

    void renameFile(IcebergFileParam icebergFileParam) throws Exception;
}
