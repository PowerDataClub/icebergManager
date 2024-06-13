package com.powerdata.system.domain.param;

import lombok.Data;

/**
 * @author dinghao
 * @version 1.0
 * @description
 * @date 2023/8/25 8:43
 */
@Data
public class TableToDataParam {
    private IcebergTableParam icebergTableParam;
    private IcebergFileParam icebergFileParam;
    private String splitStr;
    private String isHead="0";
}
