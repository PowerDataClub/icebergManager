package com.powerdata.system.domain.param;

import com.powerdata.system.domain.IcebergCatalog;
import lombok.Data;

import java.util.List;

@Data
public class IcebergTableParam extends IcebergCatalog {
    private String databaseName;
    private String tableName;
    private List<IcebergColumnDto> columnDtos;
    private List<IcebergPartitionDto> partitionParams;
}
