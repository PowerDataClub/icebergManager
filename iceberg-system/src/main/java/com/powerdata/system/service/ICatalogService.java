package com.powerdata.system.service;

import com.powerdata.system.domain.IcebergCatalog;
import com.powerdata.system.domain.param.IcebergCatalogParam;
import org.springframework.web.multipart.MultipartFile;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.List;
import java.util.Map;

/**
 * @author dinghao
 * @version 1.0
 * @description
 * @date 2023/6/12 11:10
 */
public interface ICatalogService {
    Map<String,Object> catalogList(IcebergCatalogParam icebergCatalogParam) throws Exception;

    void addCatalog(IcebergCatalog icebergCatalog) throws Exception;

    void modifyCatalog(IcebergCatalog icebergCatalog) throws Exception;

    void deleteCatalog(IcebergCatalog icebergCatalog) throws Exception;

    List<String> upLoadHiveCatalogFile(MultipartFile file, String catalogId) throws Exception;

    List<String> getHiveCatalogFiles(String catalogId) throws Exception;

    List<String> deleteHiveCatalogFiles(String catalogId, String types) throws Exception;

    void downloadHiveCatalogFile(HttpServletRequest request, HttpServletResponse response,
                                 String catalogId, String types) throws Exception;

    void initCatalogUtils();
}
