package com.powerdata.common.utils.bean.table;

import lombok.Data;

import java.util.List;

/**
 * hive 建表或更新表请求封装bean
 *
 * @author wangyongtao
 */
@Data
public class HiveCreateOrUpdateTableRequest {
    public String id;

    /** hive数据源唯一标示 */
    public String clusterCode;

    // 库名
    public String dbName;

    // 表名
    public OldNewValue<String> tableName;

    // 表注释名
    public OldNewValue<String> tableComment;

    // 表类型 （管理表 MANAGED、外部表 EXTERNAL、临时表 TEMPORARY）
    public String tableType = "MANAGED";

    // 行格式化：字段分隔符定义,默认\t
    public String fieldTerminated = "\t";

    //存储文件格式
    public OldNewValue<String> fileFormat;

    // 普通字段列表
    public List<CreateOrUpdateColumn> columns;

    // 分区字段列表
    public List<CreateOrUpdateColumn> partitions;

    // 初始化文件位置
    public String hdfsLocation;

    // 表占用空间大小（单位：KB）
    public long totalSize;

    // 表创建时间
    public String createTime;

    // 分区数
    public int numPartitions;

    // 判断表是否已有数据
    public Boolean hasData = true;

}
