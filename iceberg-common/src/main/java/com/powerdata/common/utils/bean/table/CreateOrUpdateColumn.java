package com.powerdata.common.utils.bean.table;

/**
 * hive 建表或更新表时的字段封装bean
 *
 * @author wangyongtao
 */
public class CreateOrUpdateColumn {

    // 字段名
    public OldNewValue<String> columnName;

    // 字段类型
    public OldNewValue<String> columnType;

    // 字段注释
    public OldNewValue<String> columnComment;

    // 字段默认值
    public OldNewValue<String> defaultValue;

    // 字段非空
    public  OldNewValue<Boolean> notNull;

    // 是否移动
    public Boolean moveFlag = false;

    // 是否删除
    public Boolean deleteFlag = false;


    public OldNewValue<String> getColumnName() {
        return columnName;
    }

    public void setColumnName(OldNewValue<String> columnName) {
        this.columnName = columnName;
    }

    public OldNewValue<String> getColumnType() {
        return columnType;
    }

    public void setColumnType(OldNewValue<String> columnType) {
        this.columnType = columnType;
    }

    public OldNewValue<String> getColumnComment() {
        return columnComment;
    }

    public void setColumnComment(OldNewValue<String> columnComment) {
        this.columnComment = columnComment;
    }

    public OldNewValue<String> getDefaultValue() {
        return defaultValue;
    }

    public void setDefaultValue(OldNewValue<String> defaultValue) {
        this.defaultValue = defaultValue;
    }

    public OldNewValue<Boolean> getNotNull() {
        return notNull;
    }

    public void setNotNull(OldNewValue<Boolean> notNull) {
        this.notNull = notNull;
    }

    public Boolean getMoveFlag() {
        return moveFlag;
    }

    public void setMoveFlag(Boolean moveFlag) {
        this.moveFlag = moveFlag;
    }

    public Boolean getDeleteFlag() {
        return deleteFlag;
    }

    public void setDeleteFlag(Boolean deleteFlag) {
        this.deleteFlag = deleteFlag;
    }
}
