package com.powerdata.system.domain.param;

import lombok.Data;

@Data
public class IcebergColumnDto {

    //列名
    private String columnName;
    //列名
    private String newColumnName;

    //是否可为空
    private String isNullable;
    //是否可为空
    private String newIsNullable;
    //类型
    private String dataType;
    //new类型
    private String newDataType;
    // decimal 总长度
    private String precision;
    //decimal 总长度 小数位长度
    private String scale;
    /**
     * 带时区
     */
    private String isWithZone;
    /**
     * 注释
     */
    private String comment;
    /**
     * 注释
     */
    private String newComment;
    /**
     *Map的value  或者 list 的item  是否为空
     */

    private String isNullableSubType;
    /**
     * Map的value  或者 list 的item 的类型
     */
    private String valueType;
    /**
     * key类型
     */
    private String keyType;
    //修改类型：0:不变；1：新增；2：修改；3：删除；
    private String exectype;

}