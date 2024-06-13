package com.powerdata.system.domain.param;

import lombok.Data;

@Data
public class IcebergPartitionDto {

    //列名
    private String sourceName;

    private String type;
    //列名
    private String targetName;


    private String dateType;

    private String exectype;

}