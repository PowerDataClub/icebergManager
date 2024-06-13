package com.powerdata.common.utils.bean.table;

import lombok.Data;

@Data
public class TablePartitionKeysBean {
    private Integer id;//顺序id
    private String field;//分区字段名
    private String sourceField;//来源字段
    private String transform;//类型

    public TablePartitionKeysBean(){}

    public TablePartitionKeysBean(Integer id, String field, String sourceField, String transform) {
        this.id = id;
        this.field = field;
        this.sourceField = sourceField;
        this.transform = transform;
    }
}
