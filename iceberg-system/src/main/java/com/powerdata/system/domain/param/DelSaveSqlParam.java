package com.powerdata.system.domain.param;

import lombok.Data;

import java.util.List;

/**
 * @author dinghao
 * @version 1.0
 * @description
 * @date 2023/8/25 10:32
 */
@Data
public class DelSaveSqlParam {
    private List<String> delNames;
}
