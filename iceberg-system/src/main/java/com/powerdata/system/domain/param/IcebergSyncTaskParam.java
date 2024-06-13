package com.powerdata.system.domain.param;

import com.powerdata.system.domain.IcebergSyncTask;
import lombok.Data;

/**
 * @author dinghao
 * @version 1.0
 * @description
 * @date 2023/8/29 16:14
 */
@Data
public class IcebergSyncTaskParam extends IcebergSyncTask {
    private String types;
    private String hiveUrl;
    private String hdfsUrl;
    private Integer pageSize;
    private Integer pageNum;
    private String orderByColumn = "id";
    private String isAsc = "desc";

    public IcebergSyncTaskParam(){}

    public IcebergSyncTaskParam(String types, String hiveurl, String hdfsurl, IcebergSyncTask icebergSyncTask) {
        this.types = types;
        this.hiveUrl = hiveurl;
        this.hdfsUrl = hdfsurl;
        super.setCreater(icebergSyncTask.getCreater());
        super.setCreatetime(icebergSyncTask.getCreatetime());
        super.setDistcatalogid(icebergSyncTask.getDistcatalogid());
        super.setDistdatabase(icebergSyncTask.getDistdatabase());
        super.setDisttable(icebergSyncTask.getDisttable());
        super.setId(icebergSyncTask.getId());
        super.setOther1(icebergSyncTask.getOther1());
        super.setSourceip(icebergSyncTask.getSourceip());
        super.setSourcetypes(icebergSyncTask.getSourcetypes());
        super.setStatus(icebergSyncTask.getStatus());
        super.setSyncdata(icebergSyncTask.getSyncdata());
        super.setOther2(icebergSyncTask.getOther2());
        super.setOther3(icebergSyncTask.getOther3());
        super.setOther4(icebergSyncTask.getOther4());
    }

    @Override
    public String toString() {
        return super.getSourceip()+super.getSyncdata()+super.getDistcatalogid()+super.getDistdatabase()+super.getDisttable();
    }
}
