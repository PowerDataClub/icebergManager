package com.powerdata.common.utils.excel;

import lombok.Data;

/**
 * @author dinghao
 * @version 1.0
 * @description
 * @date 2023/2/24 9:02
 */
@Data
public class TableField {
    private String fieldName;
    private String remarks;
    private String fieldType;
    private int fieldSize;
}
