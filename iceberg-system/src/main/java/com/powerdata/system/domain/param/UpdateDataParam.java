package com.powerdata.system.domain.param;

import com.powerdata.system.domain.IcebergCatalog;
import lombok.Data;

import java.util.HashMap;

/**
 * @author dinghao
 * @version 1.0
 * @description
 * @date 2023/8/25 16:39
 */
@Data
public class UpdateDataParam extends IcebergCatalog {
    private String databaseName;
    private String tableName;
    private HashMap<String,String> oldData;
    private HashMap<String,String> newData;
}
