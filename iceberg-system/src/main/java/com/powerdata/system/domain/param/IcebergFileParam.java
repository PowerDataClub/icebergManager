package com.powerdata.system.domain.param;

import lombok.Data;
import org.springframework.web.multipart.MultipartFile;

import java.util.List;

/**
 * @author dinghao
 * @version 1.0
 * @description
 * @date 2023/7/20 15:23
 */
@Data
public class IcebergFileParam {
    private String catalogId;
    private String hdfsUrl;
    private String dirPath;
    private List<MultipartFile> files;
    private List<String> fileNames;
    private String fileName;
    private String filePath;
    private String dirName;
    private String reName;

    private Integer pageSize;
    private Integer pageNum;
}
