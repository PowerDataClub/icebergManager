package com.powerdata.common.utils.bean.table;

import lombok.Data;

@Data
public class TableMetricsBean {
//    private String databaseName;//库名
//    private String tableName;//表名
    private Double tableSize = 0.0;//表大小
    private Integer files = 0;//文件数
    private Double avgFileSize = 0.0;//平均文件大小
    private Long lastCommitTime;//上次操作时间
    private Long records = 0L;//记录数

    public TableMetricsBean() {
    }

    public TableMetricsBean(Double tableSize, Integer files, Double avgFileSize, Long lastCommitTime, Long records) {
        this.tableSize = tableSize;
        this.files = files;
        this.avgFileSize = avgFileSize;
        this.lastCommitTime = lastCommitTime;
        this.records = records;
    }
}
