package com.powerdata.system.domain.param;

import com.powerdata.system.domain.IcebergSaveSql;
import lombok.Data;

/**
 * @author dinghao
 * @version 1.0
 * @description
 * @date 2023/8/24 16:33
 */
@Data
public class IcebergSaveSqlParam extends IcebergSaveSql {
    private Integer pageSize;
    private Integer pageNum;
    private String orderByColumn;
    private String isAsc = "desc";
}
