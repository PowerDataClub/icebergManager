package com.powerdata.system.domain.param;

import lombok.Data;

/**
 * @author dinghao
 * @version 1.0
 * @description
 * @date 2023/7/25 11:01
 */
@Data
public class IcebergCopyTableParam {
    private IcebergTableParam sourceTable;
    private IcebergTableParam dstTable;
    private Boolean isData = false;
}
