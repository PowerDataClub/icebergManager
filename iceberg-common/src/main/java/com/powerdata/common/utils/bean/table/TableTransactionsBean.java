package com.powerdata.common.utils.bean.table;

import lombok.Data;

@Data
public class TableTransactionsBean {
    private String transactionId;//事务id
    private String operation;//事务类型
    private Integer addFileCount;//新增文件数
    private Integer deleteFileCount;//删除文件数
    private Double addFileSize;//新增文件大小
    private Double deleteFileSize;//删除文件大小
    private Long records;//数据明细
    private Long CommitTime;//事务提交时间
    private String snapshotId;//快照id
    private String parentId;//父级快照
    private Boolean isCurrent=false;//是否是当前快照

    public TableTransactionsBean(){}

    public TableTransactionsBean(long transactionId, String operation, Integer addFileCount, Integer deleteFileCount,
                                 Double addFileSize, Double deleteFileSize, Long commitTime, long snapshotId ,String parentId) {
        this.transactionId = transactionId+"";
        this.operation = operation;
        this.addFileCount = addFileCount;
        this.deleteFileCount = deleteFileCount;
        this.addFileSize = addFileSize;
        this.deleteFileSize = deleteFileSize;
        CommitTime = commitTime;
        this.snapshotId = snapshotId+"";
        this.parentId = parentId;
    }
}
