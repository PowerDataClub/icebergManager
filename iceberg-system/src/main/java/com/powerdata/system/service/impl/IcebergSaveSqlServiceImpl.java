package com.powerdata.system.service.impl;

import cn.hutool.core.util.ObjectUtil;
import com.powerdata.common.utils.SecurityUtils;
import com.powerdata.common.utils.StringUtils;
import com.powerdata.system.domain.IcebergExecLog;
import com.powerdata.system.domain.IcebergExecLogExample;
import com.powerdata.system.domain.IcebergSaveSql;
import com.powerdata.system.domain.IcebergSaveSqlExample;
import com.powerdata.system.domain.param.DelSaveSqlParam;
import com.powerdata.system.domain.param.IcebergExecLogParam;
import com.powerdata.system.domain.param.IcebergSaveSqlParam;
import com.powerdata.system.mapper.IcebergExecLogMapper;
import com.powerdata.system.mapper.IcebergSaveSqlMapper;
import com.powerdata.system.service.IcebergSaveSqlService;
import org.springframework.stereotype.Service;
import org.springframework.util.ObjectUtils;

import javax.annotation.Resource;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author dinghao
 * @version 1.0
 * @description
 * @date 2023/8/24 16:10
 */
@Service
public class IcebergSaveSqlServiceImpl implements IcebergSaveSqlService {
    @Resource
    private IcebergSaveSqlMapper icebergSaveSqlMapper;
    @Resource
    private IcebergExecLogMapper icebergExecLogMapper;

    @Override
    public Map<String, Object> sqlList(IcebergSaveSqlParam icebergSaveSqlParam) throws Exception {
        final HashMap<String, Object> result = new HashMap<>();
        int pageSize = ObjectUtils.isEmpty(icebergSaveSqlParam.getPageSize()) ? 10:icebergSaveSqlParam.getPageSize();
        int pageNum = ObjectUtils.isEmpty(icebergSaveSqlParam.getPageNum()) ? 1:icebergSaveSqlParam.getPageNum();
        String orderByColumn = StringUtils.isEmpty(icebergSaveSqlParam.getOrderByColumn()) ?
                "name":icebergSaveSqlParam.getOrderByColumn();
        String isAsc = StringUtils.isEmpty(icebergSaveSqlParam.getIsAsc()) ? "desc":icebergSaveSqlParam.getIsAsc();

        IcebergSaveSqlExample icebergSaveSqlExample = new IcebergSaveSqlExample();
        IcebergSaveSqlExample.Criteria criteria = icebergSaveSqlExample.createCriteria();

        if(StringUtils.isNotEmpty(icebergSaveSqlParam.getName())){
            criteria.andNameLike("%"+icebergSaveSqlParam.getName()+"%");
        }
        if(StringUtils.isNotEmpty(icebergSaveSqlParam.getExecsql())){
            criteria.andExecsqlLike("%"+icebergSaveSqlParam.getExecsql()+"%");
        }
        if(StringUtils.isNotEmpty(icebergSaveSqlParam.getCreater())){
            criteria.andCreaterLike("%"+icebergSaveSqlParam.getCreater()+"%");
        }
        List<IcebergSaveSql> icebergSaveSqlList = icebergSaveSqlMapper.selectByExample(icebergSaveSqlExample);
        if(ObjectUtils.isEmpty(icebergSaveSqlList)){
            result.put("total",0);
            result.put("list",null);
            return result;
        }
        result.put("total",icebergSaveSqlList.size());

        icebergSaveSqlExample.setOrderByClause(" "+orderByColumn+" "+ isAsc+" limit "+ (pageNum-1)*pageSize+","+pageSize);
        result.put("list",icebergSaveSqlMapper.selectByExample(icebergSaveSqlExample));
        return result;
    }

    @Override
    public void add(IcebergSaveSql icebergSaveSql) throws Exception {
        if (ObjectUtil.isNotEmpty(icebergSaveSqlMapper.selectByPrimaryKey(icebergSaveSql.getName()))) {
            throw new Exception("sql名称已存在，请重新命名");
        }
        icebergSaveSql.setCreater(SecurityUtils.getLoginUser().getUsername());
        icebergSaveSql.setCreatetime(new Date().getTime()+"");
        String sql = icebergSaveSql.getExecsql().replaceAll("\n", " ");
        while(sql.contains("  ")){
            sql = sql.replaceAll("  "," ");
        }
        icebergSaveSql.setExecsql(sql.replaceAll(";",";\n"));
        icebergSaveSqlMapper.insertSelective(icebergSaveSql);
    }

    @Override
    public void modify(IcebergSaveSql icebergSaveSql) throws Exception {
        icebergSaveSql.setCreater(SecurityUtils.getLoginUser().getUsername());
        icebergSaveSql.setCreatetime(new Date().getTime()+"");
        String sql = icebergSaveSql.getExecsql().replaceAll("\n", " ");
        while(sql.contains("  ")){
            sql = sql.replaceAll("  "," ");
        }
        icebergSaveSql.setExecsql(sql.replaceAll(";",";\n"));
        icebergSaveSqlMapper.updateByPrimaryKey(icebergSaveSql);
    }

    @Override
    public void delete(DelSaveSqlParam delSaveSqlParam) throws Exception {
        IcebergSaveSqlExample icebergSaveSqlExample = new IcebergSaveSqlExample();
        icebergSaveSqlExample.createCriteria().andNameIn(delSaveSqlParam.getDelNames());
        icebergSaveSqlMapper.deleteByExample(icebergSaveSqlExample);
    }

    @Override
    public Map<String, Object> sqlHistoryList(IcebergExecLogParam icebergExecLogParam) throws Exception {
        HashMap<String, Object> result = new HashMap<>();
        int pageSize = ObjectUtils.isEmpty(icebergExecLogParam.getPageSize()) ? 10:icebergExecLogParam.getPageSize();
        int pageNum = ObjectUtils.isEmpty(icebergExecLogParam.getPageNum()) ? 1:icebergExecLogParam.getPageNum();
        String orderByColumn = StringUtils.isEmpty(icebergExecLogParam.getOrderByColumn()) ?
                "id":icebergExecLogParam.getOrderByColumn();
        String isAsc = StringUtils.isEmpty(icebergExecLogParam.getIsAsc()) ? "desc":icebergExecLogParam.getIsAsc();

        IcebergExecLogExample icebergExecLogExample = new IcebergExecLogExample();
        IcebergExecLogExample.Criteria criteria = icebergExecLogExample.createCriteria();

        if(StringUtils.isNotEmpty(icebergExecLogParam.getSqlstr())){
            criteria.andSqlstrLike("%"+icebergExecLogParam.getSqlstr()+"%");
        }
        if(StringUtils.isNotEmpty(icebergExecLogParam.getStatus())){
            criteria.andStatusEqualTo(icebergExecLogParam.getStatus());
        }
        if(StringUtils.isNotEmpty(icebergExecLogParam.getCreater())){
            criteria.andCreaterLike("%"+icebergExecLogParam.getCreater()+"%");
        }
        List<IcebergExecLog> icebergSaveSqlList = icebergExecLogMapper.selectByExample(icebergExecLogExample);
        if(ObjectUtils.isEmpty(icebergSaveSqlList)){
            result.put("total",0);
            result.put("list",null);
            return result;
        }
        result.put("total",icebergSaveSqlList.size());

        icebergExecLogExample.setOrderByClause(" "+orderByColumn+" "+ isAsc+" limit "+ (pageNum-1)*pageSize+","+pageSize);
        result.put("list",icebergExecLogMapper.selectByExample(icebergExecLogExample));
        return result;
    }
}
