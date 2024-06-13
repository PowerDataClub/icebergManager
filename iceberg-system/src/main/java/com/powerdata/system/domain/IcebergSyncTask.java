package com.powerdata.system.domain;

public class IcebergSyncTask {
    /**
     * This field was generated by MyBatis Generator.
     * This field corresponds to the database column iceberg_sync_task.id
     *
     * @mbggenerated Tue Aug 29 16:07:24 CST 2023
     */
    private String id;

    /**
     * This field was generated by MyBatis Generator.
     * This field corresponds to the database column iceberg_sync_task.sourcetypes
     *
     * @mbggenerated Tue Aug 29 16:07:24 CST 2023
     */
    private String sourcetypes;

    /**
     * This field was generated by MyBatis Generator.
     * This field corresponds to the database column iceberg_sync_task.sourceip
     *
     * @mbggenerated Tue Aug 29 16:07:24 CST 2023
     */
    private String sourceip;

    /**
     * This field was generated by MyBatis Generator.
     * This field corresponds to the database column iceberg_sync_task.syncdata
     *
     * @mbggenerated Tue Aug 29 16:07:24 CST 2023
     */
    private String syncdata;

    /**
     * This field was generated by MyBatis Generator.
     * This field corresponds to the database column iceberg_sync_task.distcatalogid
     *
     * @mbggenerated Tue Aug 29 16:07:24 CST 2023
     */
    private String distcatalogid;

    /**
     * This field was generated by MyBatis Generator.
     * This field corresponds to the database column iceberg_sync_task.distdatabase
     *
     * @mbggenerated Tue Aug 29 16:07:24 CST 2023
     */
    private String distdatabase;

    /**
     * This field was generated by MyBatis Generator.
     * This field corresponds to the database column iceberg_sync_task.disttable
     *
     * @mbggenerated Tue Aug 29 16:07:24 CST 2023
     */
    private String disttable;

    /**
     * This field was generated by MyBatis Generator.
     * This field corresponds to the database column iceberg_sync_task.exectimes
     *
     * @mbggenerated Tue Aug 29 16:07:24 CST 2023
     */
    private String exectimes;

    /**
     * This field was generated by MyBatis Generator.
     * This field corresponds to the database column iceberg_sync_task.status
     *
     * @mbggenerated Tue Aug 29 16:07:24 CST 2023
     */
    private String status;

    /**
     * This field was generated by MyBatis Generator.
     * This field corresponds to the database column iceberg_sync_task.creater
     *
     * @mbggenerated Tue Aug 29 16:07:24 CST 2023
     */
    private String creater;

    /**
     * This field was generated by MyBatis Generator.
     * This field corresponds to the database column iceberg_sync_task.createtime
     *
     * @mbggenerated Tue Aug 29 16:07:24 CST 2023
     */
    private String createtime;

    /**
     * This field was generated by MyBatis Generator.
     * This field corresponds to the database column iceberg_sync_task.other1
     *
     * @mbggenerated Tue Aug 29 16:07:24 CST 2023
     */
    private String other1;

    /**
     * This field was generated by MyBatis Generator.
     * This field corresponds to the database column iceberg_sync_task.other2
     *
     * @mbggenerated Tue Aug 29 16:07:24 CST 2023
     */
    private String other2;

    /**
     * This field was generated by MyBatis Generator.
     * This field corresponds to the database column iceberg_sync_task.other3
     *
     * @mbggenerated Tue Aug 29 16:07:24 CST 2023
     */
    private String other3;

    /**
     * This field was generated by MyBatis Generator.
     * This field corresponds to the database column iceberg_sync_task.other4
     *
     * @mbggenerated Tue Aug 29 16:07:24 CST 2023
     */
    private String other4;

    /**
     * This method was generated by MyBatis Generator.
     * This method returns the value of the database column iceberg_sync_task.id
     *
     * @return the value of iceberg_sync_task.id
     *
     * @mbggenerated Tue Aug 29 16:07:24 CST 2023
     */
    public String getId() {
        return id;
    }

    /**
     * This method was generated by MyBatis Generator.
     * This method sets the value of the database column iceberg_sync_task.id
     *
     * @param id the value for iceberg_sync_task.id
     *
     * @mbggenerated Tue Aug 29 16:07:24 CST 2023
     */
    public void setId(String id) {
        this.id = id == null ? null : id.trim();
    }

    /**
     * This method was generated by MyBatis Generator.
     * This method returns the value of the database column iceberg_sync_task.sourcetypes
     *
     * @return the value of iceberg_sync_task.sourcetypes
     *
     * @mbggenerated Tue Aug 29 16:07:24 CST 2023
     */
    public String getSourcetypes() {
        return sourcetypes;
    }

    /**
     * This method was generated by MyBatis Generator.
     * This method sets the value of the database column iceberg_sync_task.sourcetypes
     *
     * @param sourcetypes the value for iceberg_sync_task.sourcetypes
     *
     * @mbggenerated Tue Aug 29 16:07:24 CST 2023
     */
    public void setSourcetypes(String sourcetypes) {
        this.sourcetypes = sourcetypes == null ? null : sourcetypes.trim();
    }

    /**
     * This method was generated by MyBatis Generator.
     * This method returns the value of the database column iceberg_sync_task.sourceip
     *
     * @return the value of iceberg_sync_task.sourceip
     *
     * @mbggenerated Tue Aug 29 16:07:24 CST 2023
     */
    public String getSourceip() {
        return sourceip;
    }

    /**
     * This method was generated by MyBatis Generator.
     * This method sets the value of the database column iceberg_sync_task.sourceip
     *
     * @param sourceip the value for iceberg_sync_task.sourceip
     *
     * @mbggenerated Tue Aug 29 16:07:24 CST 2023
     */
    public void setSourceip(String sourceip) {
        this.sourceip = sourceip == null ? null : sourceip.trim();
    }

    /**
     * This method was generated by MyBatis Generator.
     * This method returns the value of the database column iceberg_sync_task.syncdata
     *
     * @return the value of iceberg_sync_task.syncdata
     *
     * @mbggenerated Tue Aug 29 16:07:24 CST 2023
     */
    public String getSyncdata() {
        return syncdata;
    }

    /**
     * This method was generated by MyBatis Generator.
     * This method sets the value of the database column iceberg_sync_task.syncdata
     *
     * @param syncdata the value for iceberg_sync_task.syncdata
     *
     * @mbggenerated Tue Aug 29 16:07:24 CST 2023
     */
    public void setSyncdata(String syncdata) {
        this.syncdata = syncdata == null ? null : syncdata.trim();
    }

    /**
     * This method was generated by MyBatis Generator.
     * This method returns the value of the database column iceberg_sync_task.distcatalogid
     *
     * @return the value of iceberg_sync_task.distcatalogid
     *
     * @mbggenerated Tue Aug 29 16:07:24 CST 2023
     */
    public String getDistcatalogid() {
        return distcatalogid;
    }

    /**
     * This method was generated by MyBatis Generator.
     * This method sets the value of the database column iceberg_sync_task.distcatalogid
     *
     * @param distcatalogid the value for iceberg_sync_task.distcatalogid
     *
     * @mbggenerated Tue Aug 29 16:07:24 CST 2023
     */
    public void setDistcatalogid(String distcatalogid) {
        this.distcatalogid = distcatalogid == null ? null : distcatalogid.trim();
    }

    /**
     * This method was generated by MyBatis Generator.
     * This method returns the value of the database column iceberg_sync_task.distdatabase
     *
     * @return the value of iceberg_sync_task.distdatabase
     *
     * @mbggenerated Tue Aug 29 16:07:24 CST 2023
     */
    public String getDistdatabase() {
        return distdatabase;
    }

    /**
     * This method was generated by MyBatis Generator.
     * This method sets the value of the database column iceberg_sync_task.distdatabase
     *
     * @param distdatabase the value for iceberg_sync_task.distdatabase
     *
     * @mbggenerated Tue Aug 29 16:07:24 CST 2023
     */
    public void setDistdatabase(String distdatabase) {
        this.distdatabase = distdatabase == null ? null : distdatabase.trim();
    }

    /**
     * This method was generated by MyBatis Generator.
     * This method returns the value of the database column iceberg_sync_task.disttable
     *
     * @return the value of iceberg_sync_task.disttable
     *
     * @mbggenerated Tue Aug 29 16:07:24 CST 2023
     */
    public String getDisttable() {
        return disttable;
    }

    /**
     * This method was generated by MyBatis Generator.
     * This method sets the value of the database column iceberg_sync_task.disttable
     *
     * @param disttable the value for iceberg_sync_task.disttable
     *
     * @mbggenerated Tue Aug 29 16:07:24 CST 2023
     */
    public void setDisttable(String disttable) {
        this.disttable = disttable == null ? null : disttable.trim();
    }

    /**
     * This method was generated by MyBatis Generator.
     * This method returns the value of the database column iceberg_sync_task.exectimes
     *
     * @return the value of iceberg_sync_task.exectimes
     *
     * @mbggenerated Tue Aug 29 16:07:24 CST 2023
     */
    public String getExectimes() {
        return exectimes;
    }

    /**
     * This method was generated by MyBatis Generator.
     * This method sets the value of the database column iceberg_sync_task.exectimes
     *
     * @param exectimes the value for iceberg_sync_task.exectimes
     *
     * @mbggenerated Tue Aug 29 16:07:24 CST 2023
     */
    public void setExectimes(String exectimes) {
        this.exectimes = exectimes == null ? null : exectimes.trim();
    }

    /**
     * This method was generated by MyBatis Generator.
     * This method returns the value of the database column iceberg_sync_task.status
     *
     * @return the value of iceberg_sync_task.status
     *
     * @mbggenerated Tue Aug 29 16:07:24 CST 2023
     */
    public String getStatus() {
        return status;
    }

    /**
     * This method was generated by MyBatis Generator.
     * This method sets the value of the database column iceberg_sync_task.status
     *
     * @param status the value for iceberg_sync_task.status
     *
     * @mbggenerated Tue Aug 29 16:07:24 CST 2023
     */
    public void setStatus(String status) {
        this.status = status == null ? null : status.trim();
    }

    /**
     * This method was generated by MyBatis Generator.
     * This method returns the value of the database column iceberg_sync_task.creater
     *
     * @return the value of iceberg_sync_task.creater
     *
     * @mbggenerated Tue Aug 29 16:07:24 CST 2023
     */
    public String getCreater() {
        return creater;
    }

    /**
     * This method was generated by MyBatis Generator.
     * This method sets the value of the database column iceberg_sync_task.creater
     *
     * @param creater the value for iceberg_sync_task.creater
     *
     * @mbggenerated Tue Aug 29 16:07:24 CST 2023
     */
    public void setCreater(String creater) {
        this.creater = creater == null ? null : creater.trim();
    }

    /**
     * This method was generated by MyBatis Generator.
     * This method returns the value of the database column iceberg_sync_task.createtime
     *
     * @return the value of iceberg_sync_task.createtime
     *
     * @mbggenerated Tue Aug 29 16:07:24 CST 2023
     */
    public String getCreatetime() {
        return createtime;
    }

    /**
     * This method was generated by MyBatis Generator.
     * This method sets the value of the database column iceberg_sync_task.createtime
     *
     * @param createtime the value for iceberg_sync_task.createtime
     *
     * @mbggenerated Tue Aug 29 16:07:24 CST 2023
     */
    public void setCreatetime(String createtime) {
        this.createtime = createtime == null ? null : createtime.trim();
    }

    /**
     * This method was generated by MyBatis Generator.
     * This method returns the value of the database column iceberg_sync_task.other1
     *
     * @return the value of iceberg_sync_task.other1
     *
     * @mbggenerated Tue Aug 29 16:07:24 CST 2023
     */
    public String getOther1() {
        return other1;
    }

    /**
     * This method was generated by MyBatis Generator.
     * This method sets the value of the database column iceberg_sync_task.other1
     *
     * @param other1 the value for iceberg_sync_task.other1
     *
     * @mbggenerated Tue Aug 29 16:07:24 CST 2023
     */
    public void setOther1(String other1) {
        this.other1 = other1 == null ? null : other1.trim();
    }

    /**
     * This method was generated by MyBatis Generator.
     * This method returns the value of the database column iceberg_sync_task.other2
     *
     * @return the value of iceberg_sync_task.other2
     *
     * @mbggenerated Tue Aug 29 16:07:24 CST 2023
     */
    public String getOther2() {
        return other2;
    }

    /**
     * This method was generated by MyBatis Generator.
     * This method sets the value of the database column iceberg_sync_task.other2
     *
     * @param other2 the value for iceberg_sync_task.other2
     *
     * @mbggenerated Tue Aug 29 16:07:24 CST 2023
     */
    public void setOther2(String other2) {
        this.other2 = other2 == null ? null : other2.trim();
    }

    /**
     * This method was generated by MyBatis Generator.
     * This method returns the value of the database column iceberg_sync_task.other3
     *
     * @return the value of iceberg_sync_task.other3
     *
     * @mbggenerated Tue Aug 29 16:07:24 CST 2023
     */
    public String getOther3() {
        return other3;
    }

    /**
     * This method was generated by MyBatis Generator.
     * This method sets the value of the database column iceberg_sync_task.other3
     *
     * @param other3 the value for iceberg_sync_task.other3
     *
     * @mbggenerated Tue Aug 29 16:07:24 CST 2023
     */
    public void setOther3(String other3) {
        this.other3 = other3 == null ? null : other3.trim();
    }

    /**
     * This method was generated by MyBatis Generator.
     * This method returns the value of the database column iceberg_sync_task.other4
     *
     * @return the value of iceberg_sync_task.other4
     *
     * @mbggenerated Tue Aug 29 16:07:24 CST 2023
     */
    public String getOther4() {
        return other4;
    }

    /**
     * This method was generated by MyBatis Generator.
     * This method sets the value of the database column iceberg_sync_task.other4
     *
     * @param other4 the value for iceberg_sync_task.other4
     *
     * @mbggenerated Tue Aug 29 16:07:24 CST 2023
     */
    public void setOther4(String other4) {
        this.other4 = other4 == null ? null : other4.trim();
    }
}