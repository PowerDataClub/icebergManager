package com.powerdata.common.enums;

import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

public enum IcebergBaseType {

    STRING(Types.StringType.get()),
    FLOAT(Types.FloatType.get()),
    DOUBLE(Types.DoubleType.get()),
    LONG(Types.LongType.get()),
    INTEGER(Types.IntegerType.get()),
    BOOLEAN(Types.BooleanType.get()),
    DATE(Types.DateType.get()),
    ;


    public  Type type;

    IcebergBaseType(Type type) {
        this.type = type;
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public static IcebergBaseType getType(String name) {
        try {
            if ((name = name.trim()).startsWith("int")){
                name = "integer";
            }
            return IcebergBaseType.valueOf(name.toUpperCase());
        } catch (Exception e) {
            return null;
        }

    }

}
