package com.powerdata.system.domain.param;

import com.powerdata.system.domain.IcebergExecLog;
import lombok.Data;

/**
 * @author dinghao
 * @version 1.0
 * @description
 * @date 2023/8/24 16:58
 */
@Data
public class IcebergExecLogParam extends IcebergExecLog {
    private Integer pageSize;
    private Integer pageNum;
    private String orderByColumn;
    private String isAsc = "desc";
}
