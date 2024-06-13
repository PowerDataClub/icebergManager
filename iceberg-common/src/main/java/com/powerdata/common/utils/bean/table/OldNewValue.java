package com.powerdata.common.utils.bean.table;

/**
 * hive 值封装bean
 *
 * @author wangyongtao
 */
public class OldNewValue<T> {

    public T oldValue;

    public T newValue;

    public OldNewValue() {
    }

    public OldNewValue(T oldValue, T newValue) {
        this.oldValue = oldValue;
        this.newValue = newValue;
    }

    // 字段列渲染类型：input select
//    public String renderType = "input";

    // 字段约束唯一标识，目前只有非空和默认值对象中该字段有意义
    public String constraintName ;

    public T getOldValue() {
        return oldValue;
    }

    public void setOldValue(T oldValue) {
        this.oldValue = oldValue;
    }

    public T getNewValue() {
        return newValue;
    }

    public void setNewValue(T newValue) {
        this.newValue = newValue;
    }

//    public String getRenderType() {
//        return renderType;
//    }
//
//    public void setRenderType(String renderType) {
//        this.renderType = renderType;
//    }

    public String getConstraintName() {
        return constraintName;
    }

    public void setConstraintName(String constraintName) {
        this.constraintName = constraintName;
    }
}
