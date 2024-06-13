package com.powerdata.common.utils.excel;

import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
public class ExcelSheetData {
    private String excelLable;
    private List<List<String>> data;
    private List<TableField> fields;
    private boolean isSheet = true;
    private List<Map<String, Object>> jsonArray;
    private String datasetName;
    private String sheetExcelId;
    private String id;
    private String path;
    private String fieldsMd5;
    private Boolean changeFiled = false;
    private Boolean effectExtField = false;
}
