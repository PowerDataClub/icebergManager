package com.powerdata.common.utils.iceberg;

import java.util.List;
import java.util.Map;

public abstract class PDIcebergExecUtils {
    public abstract List<Map<String,Object>> executeSql(String sql, String flag);
    public abstract List<Map<String,Object>> executeSql(String sql);
}
