package com.powerdata.common.utils.bean;

import lombok.Data;

/**
 * @author dinghao
 * @version 1.0
 * @description
 * @date 2023/7/20 13:58
 */
@Data
public class HdfsFileObject {
    private String fileName;
    private Boolean isFile;
    private Long fileSize;
    private Long updateTime;

    public HdfsFileObject(){}

    public HdfsFileObject(String fileName, Boolean isFile, Long fileSize, Long updateTime) {
        this.fileName = fileName;
        this.isFile = isFile;
        this.fileSize = fileSize;
        this.updateTime = updateTime;
    }
}
