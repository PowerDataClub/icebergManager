package com.powerdata.system.service.impl;

import com.powerdata.common.utils.StringUtils;
import com.powerdata.common.utils.file.FileUtils;
import com.powerdata.common.utils.iceberg.PDHdfsUtils;
import com.powerdata.common.utils.iceberg.PDIcebergFlinkUtils;
import com.powerdata.common.utils.iceberg.PDIcebergSparkUtils;
import com.powerdata.common.utils.iceberg.PDIcebergUtils;
import com.powerdata.system.domain.IcebergCatalog;
import com.powerdata.system.domain.IcebergCatalogExample;
import com.powerdata.system.domain.param.IcebergCatalogParam;
import com.powerdata.system.mapper.IcebergCatalogMapper;
import com.powerdata.system.service.ICatalogService;
import jodd.io.FileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.ObjectUtils;
import org.springframework.web.multipart.MultipartFile;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

import static com.powerdata.common.utils.SecurityUtils.getUsername;

/**
 * @author dinghao
 * @version 1.0
 * @description
 * @date 2023/6/12 11:10
 */
@Service
public class CatalogServiceImpl implements ICatalogService {

    private static final Logger log = LoggerFactory.getLogger(CatalogServiceImpl.class);

    @Value(value = "${icebergManager.hiveConf}")
    private String uploadHiveFilePath;

    @Value(value = "${icebergManager.hadoopUser}")
    private String hadoopUser;

    @Resource
    private IcebergCatalogMapper icebergCatalogMapper;

    @Override
    public Map<String, Object> catalogList(IcebergCatalogParam icebergCatalogParam){
        HashMap<String, Object> result = new HashMap<>();
        int pageSize = ObjectUtils.isEmpty(icebergCatalogParam.getPageSize()) ? 10:icebergCatalogParam.getPageSize();
        int pageNum = ObjectUtils.isEmpty(icebergCatalogParam.getPageNum()) ? 1:icebergCatalogParam.getPageNum();
        String orderByColumn =
                StringUtils.isEmpty(icebergCatalogParam.getOrderByColumn()) ? "id":icebergCatalogParam.getOrderByColumn();
        String isAsc = StringUtils.isEmpty(icebergCatalogParam.getIsAsc()) ? "desc":icebergCatalogParam.getIsAsc();
        IcebergCatalogExample icebergCatalogExample = new IcebergCatalogExample();
        IcebergCatalogExample.Criteria criteria = icebergCatalogExample.createCriteria();
        if(StringUtils.isNotEmpty(icebergCatalogParam.getId())){
            criteria.andIdLike("%"+icebergCatalogParam.getId()+"%");
        }
        if(StringUtils.isNotEmpty(icebergCatalogParam.getDescs())){
            criteria.andDescsLike("%"+icebergCatalogParam.getDescs()+"%");
        }
        if(StringUtils.isNotEmpty(icebergCatalogParam.getTypes())){
            criteria.andTypesLike("%"+icebergCatalogParam.getTypes()+"%");
        }
        if(StringUtils.isNotEmpty(icebergCatalogParam.getHiveurl())){
            criteria.andHiveurlLike("%"+icebergCatalogParam.getHiveurl()+"%");
        }
        if(StringUtils.isNotEmpty(icebergCatalogParam.getHdfsurl())){
            criteria.andHdfsurlLike("%"+icebergCatalogParam.getHdfsurl()+"%");
        }
        List<IcebergCatalog> icebergCatalogs = icebergCatalogMapper.selectByExample(icebergCatalogExample);
        if(ObjectUtils.isEmpty(icebergCatalogs)){
            result.put("total",0);
            result.put("list",null);
            return result;
        }
        result.put("total",icebergCatalogs.size());

        icebergCatalogExample.setOrderByClause(" "+orderByColumn+" "+ isAsc+" limit "+ (pageNum-1)*pageSize+","+pageSize);
        result.put("list",icebergCatalogMapper.selectByExample(icebergCatalogExample));
        return result;
    }

    @Override
    public void addCatalog(IcebergCatalog icebergCatalog) throws Exception {
        String catalogId = icebergCatalog.getId();
        if (!ObjectUtils.isEmpty(icebergCatalogMapper.selectByPrimaryKey(catalogId))) {
            throw new Exception("catalog的id已存在");
        }
        checkIcebergParam(icebergCatalog, catalogId);
        try {
            checkAndInitCatalogForUtils(icebergCatalog);
        } catch (Exception e){
            removeCatalogForUtils(catalogId);
            throw new Exception("请重新确认目录配置路径信息是否正确，新增目录初始化失败："+e);
        }
        long time = new Date().getTime();
        String username = getUsername();
        icebergCatalog.setCreataby(username);
        icebergCatalog.setCreatetime(time+"");
        icebergCatalog.setModifyby(username);
        icebergCatalog.setModifytime(time+"");
        icebergCatalogMapper.insertSelective(icebergCatalog);
    }

    @Override
    @Transactional
    public void modifyCatalog(IcebergCatalog icebergCatalog) throws Exception {
        String catalogId = icebergCatalog.getId();
        if (ObjectUtils.isEmpty(icebergCatalogMapper.selectByPrimaryKey(catalogId))) {
            throw new Exception("该catalog已删除");
        }
        checkIcebergParam(icebergCatalog, catalogId);
        try {
            removeCatalogForUtils(catalogId);
            checkAndInitCatalogForUtils(icebergCatalog);
        } catch (Exception e){
            removeCatalogForUtils(catalogId);
            throw new Exception("请重新确认目录配置路径信息是否正确，新增目录初始化失败："+e);
        } finally {
            IcebergCatalog oldIcebergCatalog = icebergCatalogMapper.selectByPrimaryKey(catalogId);
            try {
                checkAndInitCatalogForUtils(oldIcebergCatalog);
            } catch (Exception e2){
                e2.printStackTrace();
            }
        }
        long time = new Date().getTime();
        String username = getUsername();
        icebergCatalog.setModifyby(username);
        icebergCatalog.setModifytime(time+"");
        icebergCatalogMapper.updateByPrimaryKeySelective(icebergCatalog);
        if("file".equals(icebergCatalog.getTypes())){
            PDHdfsUtils.fileSystemMap.remove(catalogId);
        }else{
            PDIcebergUtils.hadoopCatalogHashMap.remove(icebergCatalog.getId());
        }
    }

    private void checkIcebergParam(IcebergCatalog icebergCatalog, String catalogId) throws Exception {
        if (icebergCatalog.getHdfsurl().endsWith("/")) {
            icebergCatalog.setHdfsurl(icebergCatalog.getHdfsurl().substring(0,icebergCatalog.getHdfsurl().length()-1));
        }
        if("hive".equals(icebergCatalog.getTypes())){
            List<String> hiveCatalogFiles = getHiveCatalogFiles(catalogId);
            if(ObjectUtils.isEmpty(hiveCatalogFiles)||hiveCatalogFiles.size()<3){
                throw new Exception("请上传完整hiveCatalog配置文件（hive-site.xml、core-site.xml、hdfs-site.xml）");
            }
        }
    }

    @Override
    @Transactional
    public void deleteCatalog(IcebergCatalog icebergCatalog) throws Exception {
        String catalogId = icebergCatalog.getId();
        if (ObjectUtils.isEmpty(icebergCatalogMapper.selectByPrimaryKey(catalogId))) {
            throw new Exception("该catalog已删除");
        }
        icebergCatalogMapper.deleteByPrimaryKey(catalogId);
        if("hive".equals(icebergCatalog.getTypes())) {
            deleteAllHiveCatalogFiles(icebergCatalog.getId());
        }
        removeCatalogForUtils(catalogId);
    }

    @Override
    public List<String> upLoadHiveCatalogFile(MultipartFile file, String catalogId) throws Exception {
        String srcPath = uploadHiveFilePath +catalogId+"/";
        String fileName = file.getOriginalFilename();
        String dstStr = srcPath + fileName;
        File dir = new File(srcPath);
        if(!(dir.exists() && dir.isDirectory())){
            dir.mkdirs();
        }
        File dstFile = new File(dstStr);
        if (dstFile.exists()) {
            dstFile.delete();
        }
        try {
            file.transferTo(dstFile);
            return Arrays.stream(Objects.requireNonNull(dir.listFiles())).map(File::getName).collect(Collectors.toList());
        } catch (FileNotFoundException e) {
            throw new IOException(e.getMessage());
        }
    }

    @Override
    public List<String> getHiveCatalogFiles(String catalogId) throws Exception {
        String srcPath = uploadHiveFilePath +catalogId+"/";
        File dir = new File(srcPath);
        if(!(dir.exists() && dir.isDirectory())){
            dir.mkdirs();
        }
        return Arrays.stream(Objects.requireNonNull(dir.listFiles())).map(File::getName).collect(Collectors.toList());
    }

    @Override
    public List<String> deleteHiveCatalogFiles(String catalogId, String types) throws Exception {
        List<String> typeList = Arrays.asList("core", "hdfs", "hive");
        if(!typeList.contains(types)){
            throw new Exception("文件类型不存在");
        }
        File dir = new File(uploadHiveFilePath + catalogId);
        String deleteFile = uploadHiveFilePath +catalogId+"/"+types+"-site.xml";
        File file = new File(deleteFile);
        FileUtil.delete(file);
        return Arrays.stream(Objects.requireNonNull(dir.listFiles())).map(File::getName).collect(Collectors.toList());
    }

    public void deleteAllHiveCatalogFiles(String catalogId) throws Exception {
        List<String> typeList = Arrays.asList("core", "hdfs", "hive");
        for (String types : typeList) {
            String deleteFile = uploadHiveFilePath +catalogId+"/"+types+"-site.xml";
            File file = new File(deleteFile);
            System.out.println(deleteFile);
            file.delete();
        }
    }

    @Override
    public void downloadHiveCatalogFile(HttpServletRequest request, HttpServletResponse response,
                                        String catalogId, String types) throws Exception {
        String downloadFile = uploadHiveFilePath +catalogId+"/"+types+"-site.xml";
        File file = new File(downloadFile);
        if (!file.exists() || !file.isFile()) {
            throw new Exception("文件已不存在，无法下载");
        }
        try {
            FileUtils.download(request, response, downloadFile, types+"-site.xml");
        } catch (Exception e) {
            e.printStackTrace();
            throw new Exception(e.getMessage());
        }
    }

    private void removeCatalogForUtils(String catalogId) {
        PDIcebergFlinkUtils.icebergFlinkClientsMap.remove(catalogId);
        PDIcebergSparkUtils.icebergSparkClientsMap.remove(catalogId);
        PDIcebergUtils.hadoopCatalogHashMap.remove(catalogId);
        PDHdfsUtils.fileSystemMap.remove(catalogId);
    }

    private void checkAndInitCatalogForUtils(IcebergCatalog icebergCatalog) throws Exception {
        String catalogId = icebergCatalog.getId();
        String types = icebergCatalog.getTypes();
        String hdfsUrl = icebergCatalog.getHdfsurl();
        String hiveUrl = icebergCatalog.getHiveurl();
        if("file".equals(types)){
            PDHdfsUtils.build(catalogId,hdfsUrl,hadoopUser);
        }else{
            PDIcebergUtils build = PDIcebergUtils.build(catalogId, types, hiveUrl, hdfsUrl,hadoopUser,uploadHiveFilePath);
            build.listDataBases();
            PDIcebergSparkUtils.build(catalogId,types,hiveUrl,hdfsUrl,null,hadoopUser);
            PDIcebergFlinkUtils.build(catalogId,types,hiveUrl,hdfsUrl,null,hadoopUser);
        }
    }

    @Override
    public void initCatalogUtils(){
        log.info("初始化目录开始");
        IcebergCatalogExample icebergCatalogExample = new IcebergCatalogExample();
        List<IcebergCatalog> icebergCatalogs = icebergCatalogMapper.selectByExample(icebergCatalogExample);
        for (IcebergCatalog icebergCatalog : icebergCatalogs) {
            try {
                log.info("开始初始化目录【"+icebergCatalog.getId()+"】");
                checkAndInitCatalogForUtils(icebergCatalog);
                log.info("初始化目录【"+icebergCatalog.getId()+"】成功");
            } catch (Exception e){
                log.info("初始化目录【"+icebergCatalog.getId()+"】失败"+e.getMessage());
                e.printStackTrace();
            }
        }
        log.info("初始化目录完成");
    }
}
