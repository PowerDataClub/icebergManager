package com.powerdata.common.utils.bean.table;

import lombok.Data;

@Data
public class TableColumnsBean {
    private Integer id;//id序号
    private String columnName;//字段名
    private String dataType;//类型
    private String comment;//注释
    //是否可为空
    private String isNullable;

    public TableColumnsBean(){}
    public TableColumnsBean(Integer id, String fieldName, String types, String description,boolean isOptional) {
        this.id = id;
        this.columnName = fieldName;
        this.dataType = types;
        this.comment = description;
        this.isNullable = isOptional?"0":"1";
    }
}
