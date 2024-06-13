package com.powerdata.common.utils.iceberg;


import cn.hutool.core.collection.ConcurrentHashSet;
import cn.hutool.core.io.IoUtil;
import cn.hutool.core.thread.ThreadUtil;
import cn.hutool.core.util.ZipUtil;
import cn.hutool.poi.excel.ExcelUtil;
import com.powerdata.common.exception.base.BaseException;
import com.powerdata.common.utils.bean.HdfsFileObject;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.poi.xslf.usermodel.*;
import org.apache.poi.xwpf.usermodel.XWPFDocument;
import org.apache.poi.xwpf.usermodel.XWPFParagraph;
import org.springframework.util.ObjectUtils;
import org.springframework.web.multipart.MultipartFile;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.*;
import java.net.URI;
import java.util.*;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

/**
 * @author dinghao
 * @version 1.0
 * @description
 * @date 2023/7/20 13:38
 */
public class PDHdfsUtils {
    private FileSystem fileSystem;
    private String rootDir="";
    private String hdfsUrl;
    private String catalogId;
    private String execUser;
    public static HashMap<String, PDHdfsUtils> fileSystemMap = new HashMap<>();

    private PDHdfsUtils(String catalogId, String hdfsUrl,String hadoopUser) throws IOException {
        execUser = hadoopUser;
        System.setProperty("HADOOP_USER_NAME", hadoopUser);
        this.catalogId = catalogId;
        this.fileSystem = FileSystem.get(URI.create(hdfsUrl), new Configuration());
        this.hdfsUrl = hdfsUrl;
        if(!hdfsUrl.endsWith(":8020")){
            this.rootDir = hdfsUrl.split("8020")[1];
        }
    }

    public FileSystem getFileSystem(){
        return fileSystem;
    }

    public static PDHdfsUtils build(String catalogId, String hdfsUrl,String hadoopUser) throws IOException {
        PDHdfsUtils pdHdfsUtils = fileSystemMap.get(catalogId);
        if(ObjectUtils.isEmpty(pdHdfsUtils)){
            pdHdfsUtils = new PDHdfsUtils(catalogId,hdfsUrl,hadoopUser);
            fileSystemMap.put(catalogId,pdHdfsUtils);
        }
        return pdHdfsUtils;
    }

    public Map<String,Object> getFileList(String dirPath,String fileName, Integer pageSize, Integer pageNum) throws IOException {
        HashMap<String,Object> resultMap = new HashMap<>();

        String filePath = rootDir+"/"+dirPath;
        List<FileStatus> fileStatuses =
                Arrays.stream(fileSystem.listStatus(new Path(filePath)))
                        .filter(file -> StringUtils.isEmpty(fileName) || file.getPath().getName().contains(fileName))
                        .collect(Collectors.toList());
        if(ObjectUtils.isEmpty(fileStatuses)) {
            resultMap.put("total", 0);
            return resultMap;
        }
        resultMap.put("total",fileStatuses.size());
        final List<HdfsFileObject> files = fileStatuses.stream()
                .skip((long) (pageNum - 1) * pageSize)
                .limit(pageSize)
                .map(fileStatus -> {
                    String fileName1 = fileStatus.getPath().getName();
                    boolean isFile = fileStatus.isFile();
                    long fileSize = fileStatus.getLen();
                    long modifyTime = fileStatus.getModificationTime();
                    return new HdfsFileObject(fileName1,isFile,fileSize,modifyTime);
                }).collect(Collectors.toList());
        resultMap.put("data",files);
        return resultMap;
    }

    public Map<String,Object> batchUploadFileToHdfs(String dirPath,List<MultipartFile> files) throws Exception {
        HashMap<String, Object> resultMap = new HashMap<>();
        String dstPath = rootDir+dirPath;
        ExecutorService executorService = ThreadUtil.newExecutor(10, 20, 20);
        CompletionService<Object> uploadFileCompletionService = ThreadUtil.newCompletionService(executorService);
        if (!fileSystem.exists(new Path(dstPath))) fileSystem.mkdirs(new Path(dstPath));

        ConcurrentHashSet<String> successFileSet = new ConcurrentHashSet<>();
        ConcurrentHashSet<String> errorFileSet = new ConcurrentHashSet<>();
        ConcurrentHashMap<String, String> errMessage = new ConcurrentHashMap<>();

        for (MultipartFile file : files) {
            String fileName = file.getOriginalFilename();
            if ("success".equals(uploadFileCompletionService.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        OutputStream out = fileSystem.create(new Path((dstPath + "/" + fileName).replaceAll("//", "/")), false);
                        InputStream in = file.getInputStream();
                        IOUtils.copyBytes(in, out, fileSystem.getConf(), true);
                    } catch (Exception e) {
                        errorFileSet.add(file.getOriginalFilename());
                        if (e.getMessage().length() > 0) {
                            errMessage.put(Objects.requireNonNull(file.getOriginalFilename()), e.getMessage().split("\n")[0]);
                        }
                    }
                }
            }, "success").get().toString())) {
                successFileSet.add(file.getOriginalFilename());
            }
        }
        uploadFileCompletionService.take();
        executorService.shutdown();
        if (errorFileSet.size() > 0) {
            successFileSet.removeAll(errorFileSet);
        }
        resultMap.put("successFiles", successFileSet);
        resultMap.put("errorFiles", errorFileSet);
        resultMap.put("errorMessage", errMessage);
        return resultMap;
    }

    public void downloadHdfsFile(HttpServletRequest request, HttpServletResponse response,String filePath, String fileName){
        OutputStream os = null;
        InputStream ins = null;
        try {
            os = response.getOutputStream();
            ins = fileSystem.open(new Path((rootDir+"/"+filePath + "/" + fileName).replaceAll("//", "/")));
            response.setContentType("application/x-download;charset=GBK");
            response.setHeader("Content-Disposition", "attachment;filename=" +
                    new String(fileName.getBytes("UTF-8"), "iso-8859-1"));
            IoUtil.copy(ins, os);
            response.flushBuffer();
        } catch (IOException i) {
            i.printStackTrace();
            throw new BaseException("文件下载传输异常");
        } finally {
            IoUtil.closeIfPosible(ins);
            IoUtil.closeIfPosible(os);
        }
    }

    public void reviewMP4AndPic(HttpServletResponse response,String filePath, String fileName) throws Exception {
        if (StringUtils.isEmpty(fileName)) {
            throw new Exception("请指定预览图片、视频文件");
        }
        if (!fileName.contains(".")) {
            throw new Exception("该文件非图片、视频文件");
        }
        String format = "." + fileName.split("\\.")[1];
        String fileAbPath = rootDir+"/"+filePath + "/" + fileName.replaceAll("//", "/");
//        long fileSize = fileSystem.getLength(new Path(fileAbPath));
        long fileSize = fileSystem.getFileStatus(new Path(fileAbPath)).getLen();
        if (!fileSystem.exists(new Path(fileAbPath)) || !fileSystem.getFileStatus(new Path(fileAbPath)).isFile()) {
            throw new Exception("图片、视频文件不存在");
        }
        FSDataInputStream open = null;
        try {
            open = fileSystem.open(new Path(fileAbPath));
            if (".jpg".equals(format)||".png".equals(format)) {
                response.flushBuffer();
                response.setContentType("application/octet-stream");
                response.setHeader("Content-Length", fileSize + "");
                response.setHeader("Content-Range", "bytes 0-" + fileSize + "/" + fileSize);
                response.setHeader("Accept-Ranges", "bytes");
                response.setHeader("Content-Disposition", "attachment; filename=" + fileName);
                IoUtil.copy(open, response.getOutputStream());
                response.flushBuffer();
            } else if (".mp4".equals(format) || ".ogg".equals(format) || ".webm".equals(format)) {
                response.flushBuffer();
//                response.setContentType("video/mp4");
                response.setContentType("application/octet-stream");
                response.setHeader("Content-Length", fileSize + "");
                response.setHeader("Content-Range", "bytes 0-" + fileSize + "/" + fileSize);
                response.setHeader("Accept-Ranges", "bytes");
                response.setHeader("Content-Disposition", "attachment; filename=" + fileName);
                IoUtil.copy(open, response.getOutputStream());
                response.flushBuffer();
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new Exception(e.getMessage());
        } finally {
            IoUtil.closeIfPosible(open);
        }
    }

    public String catFile(String filePath, String fileName) throws Exception {
        try {
            if (fileName.endsWith(".avro")) {
                return catAvroFile(hdfsUrl + filePath + "/" + fileName);
            }
            if (fileName.endsWith(".xls") || fileName.endsWith(".xlsx")) {
                return catExcelFile(hdfsUrl + filePath + "/" + fileName);
            }
            if (fileName.endsWith(".doc") || fileName.endsWith(".docx")) {
                return catWordFile(hdfsUrl + filePath + "/" + fileName);
            }
            if (fileName.endsWith(".ppt") || fileName.endsWith(".pptx")) {
                return catPPTFile(hdfsUrl + filePath + "/" + fileName);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        StringBuilder result = new StringBuilder();
        String readLine = "";
        String file = rootDir + "/" + filePath+"/"+fileName;
        FSDataInputStream open = fileSystem.open(new Path(file));
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(open));
        int i = 0;
        while((readLine = bufferedReader.readLine()) != null){
            result.append(readLine).append("\n");
            i++;
            if(i==1000){
                break;
            }
        }
        bufferedReader.close();
        open.close();
        return result.toString();
    }

    private String catExcelFile(String filePath) throws Exception {
        FSDataInputStream open = fileSystem.open(new Path(filePath));
        if (fileSystem.listStatus(new Path(filePath))[0].getLen()/1024>1024) {
            throw new Exception("excel文件数据大于1MB，请下载文件查看");
        }
        String result = ExcelUtil.getReader(open).readAsText(true);
        open.close();
        return result;
    }

    private String catWordFile(String filePath) throws IOException {
        StringBuilder result = new StringBuilder();
        FSDataInputStream open = fileSystem.open(new Path(filePath));
        XWPFDocument xwpfDocument = new XWPFDocument(open);
        for (XWPFParagraph paragraph : xwpfDocument.getParagraphs()) {
            result.append(paragraph.getText()).append("\n");
        }
        open.close();
        return result.toString();
    }

    private String catPPTFile(String filePath) throws IOException {
        String result = "";
        FSDataInputStream open = fileSystem.open(new Path(filePath));
        XMLSlideShow xmlSlideShow = new XMLSlideShow(open);
        for (XSLFSlide slide : xmlSlideShow.getSlides()) {
            for (XSLFShape shape : slide.getShapes()) {
                if(shape instanceof XSLFTextShape){
                    for (XSLFTextParagraph textParagraph : ((XSLFTextShape) shape).getTextParagraphs()) {
                        result = result+textParagraph.getText()+"\n";
                    }
                }
            }
        }
        open.close();
        return result;
    }

    public void downloadDir(HttpServletRequest request, HttpServletResponse response,String filePath, String fileName) throws IOException {
        OutputStream os = null;
        InputStream ins = null;
        //下载到本地服务器后压缩下载，性能差
//        Path sourDirPath = new Path(rootDir+"/"+filePath + "/" + fileName);
//        Path dstPath = new Path("/opt/icebergManager/download/" + filePath + "/" + fileName);
//        fileSystem.copyToLocalFile(false,sourDirPath,dstPath);
//        String dstPathStr = dstPath.toString() + "_" + new Date().getTime() + ".zip";
//        ZipUtil.zip(dstPath.toString(),dstPathStr);
//        FileUtil.fullyDelete(new File(dstPath.toString()));
        //hdfs上压缩
        String dstPathStr = rootDir+filePath + "/" + fileName + "_" + new Date().getTime() + ".zip";
        hdfsDirToZip(filePath,fileName,dstPathStr);
        try {
            os = response.getOutputStream();
            ins = new BufferedInputStream(fileSystem.open(new Path(dstPathStr)));
            response.setContentType("application/x-download;charset=GBK");
            response.setHeader("Content-Disposition", "attachment;filename=" +
                    new String((fileName+".zip").getBytes("UTF-8"), "iso-8859-1"));
            IoUtil.copy(ins, os);
            response.flushBuffer();
        } catch (IOException i) {
            i.printStackTrace();
            throw new BaseException("文件下载传输异常");
        } finally {
            IoUtil.closeIfPosible(ins);
            IoUtil.closeIfPosible(os);
            fileSystem.delete(new Path(dstPathStr),true);
        }
    }

    private void hdfsDirToZip(String filePath, String fileName,String dstPathStr) throws IOException {
        String sourceDir = rootDir+filePath + "/" + fileName;
        FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path(dstPathStr));
        BufferedOutputStream bos = new BufferedOutputStream(fsDataOutputStream);
        ArrayList<BufferedInputStream> ins = new ArrayList<>();
        ArrayList<String> paths = new ArrayList<>();
        getAllFile(new Path(sourceDir), paths,ins);
        String[] pathArr = new String[paths.size()];
        pathArr = paths.stream().map(path->path.split((rootDir+filePath + "/").replaceAll("//","/"))[1])
                .collect(Collectors.toList()).toArray(pathArr);
        BufferedInputStream[] insArr = new BufferedInputStream[paths.size()];
        insArr = ins.toArray(insArr);
        ZipUtil.zip(bos,pathArr,insArr);
    }

    public void getAllFile(Path sourceDir,ArrayList<String> paths, ArrayList<BufferedInputStream> ins) throws IOException {
        FileStatus[] fileStatuses = fileSystem.listStatus(sourceDir);
        for (FileStatus fileStatus : fileStatuses) {
            if(fileStatus.isDirectory()){
                getAllFile(fileStatus.getPath(),paths,ins);
            }else{
                paths.add(fileStatus.getPath().toString());
                ins.add(new BufferedInputStream(fileSystem.open(fileStatus.getPath())));
            }
        }
    }

//    private void addDirectoryToCompressedFile(Path directoryPath,  BufferedOutputStream bos) throws IOException {
//        FileStatus[] fileStatuses = fileSystem.listStatus(directoryPath);
//        for (FileStatus fileStatus : fileStatuses) {
//            if(fileStatus.isDirectory()){
//                addDirectoryToCompressedFile(fileStatus.getPath(),bos);
//            }else{
//                addFileToCompressedFile(fileStatus.getPath(),bos);
//            }
//        }
//    }
//
//    private void addFileToCompressedFile(Path filePath,BufferedOutputStream bos) throws IOException {
//        FSDataInputStream inputStream = fileSystem.open(filePath);
//        BufferedInputStream bufferedInputStream = new BufferedInputStream(inputStream);
//        Gzip gzip = new Gzip(bufferedInputStream, bos);
//        gzip.gzip();
//        bufferedInputStream.close();
//        inputStream.close();
//    }


    public String catFile(String filePath) throws Exception {
        StringBuilder result = new StringBuilder();
        String readLine = "";
        FSDataInputStream open = fileSystem.open(new Path(filePath));
        final BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(open));
        int i = 0;
        while((readLine = bufferedReader.readLine()) != null){
            result.append(readLine).append("\n");
            i++;
            if(i==1000){
                break;
            }
        }
        bufferedReader.close();
        open.close();
        return result.toString();
    }

    public void deleteFiles(String filePath,List<String> fileNames) throws Exception {
        for (String fileName : fileNames) {
            fileSystem.delete(new Path(rootDir+filePath+"/"+fileName),true);
        }
    }

    public void deleteFile(String filePath) throws Exception {
        fileSystem.delete(new Path(filePath),true);
    }

    public void getFiles(String dataPath,List<String> files) throws Exception {
        FileStatus[] fileStatuses = null;
        try{
            fileStatuses = fileSystem.listStatus(new Path(dataPath));
        }catch (Exception e){
            if (e.getMessage().contains("Operation category READ is not supported in state standby")) {
                fileSystem.close();
                fileSystemMap.remove(catalogId);
            }
            throw new Exception(e);

        }
        for (FileStatus fileStatus : fileStatuses) {
            if (fileStatus.isDirectory()) {
                getFiles((dataPath +"/"+ fileStatus.getPath().getName() + "/").replaceAll("//","/"),files);
            } else {
                files.add(fileStatus.getPath().toString());
            }
        }
    }

    public void mkdirDir(String dirPath, String dirName) throws Exception{
        fileSystem.mkdirs(new Path(rootDir+dirPath+"/"+dirName));
    }

    public List<String> getDirs(String databaseName, String tableName) throws Exception {
        String tablePath = rootDir + "/" + databaseName;
        return Arrays.stream(fileSystem.listStatus(new Path(tablePath))).map(file->file.getPath().getName())
                .filter(file->{
                    if(StringUtils.isEmpty(tableName)){
                        return true;
                    }
                    return file.contains(tableName);
                }).sorted().collect(Collectors.toList());
    }

    public static String catAvroFile(String FilePath) throws Exception{
        StringBuilder result = new StringBuilder();
        Path path = new Path(FilePath);
        DataFileReader<GenericRecord> reader =
                    new DataFileReader<>(new AvroFSInput(FileContext.getFileContext(),path), new GenericDatumReader<>());
        GenericRecord record = reader.next();
        while(record != null){
            result.append("\n").append(record).append(",");
            try {
                record = reader.next();
            }catch(Exception e){
                break;
            }
        }
        reader.close();
        if(StringUtils.isEmpty(result.toString())){
            return result.toString();
        }
        return "["+result.substring(0,result.length()-1)+"]";
    }

    public void moveFile(String filePath, List<String> fileNames, String dirPath) throws IOException {
        for (String fileName : fileNames) {
            String sourceFile = rootDir+filePath+"/"+fileName;
            String dstFile = rootDir + dirPath+"/"+fileName;
            fileSystem.rename(new Path(sourceFile),new Path(dstFile));
        }
    }

    public void writeToFile(String filePath, ArrayList<String> dataList) throws IOException {
        String fileAllPath = rootDir + filePath;
        FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path(fileAllPath));
        BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(fsDataOutputStream));
        for (String data : dataList) {
            bufferedWriter.write(data);
            bufferedWriter.newLine();
        }
        bufferedWriter.close();
        fsDataOutputStream.close();
    }

    public FSDataInputStream getFileInputStream(String filePath) throws IOException {
        String file = rootDir + filePath;
        return fileSystem.open(new Path(file));
    }

    public void renameFile(String dirPath, String fileName, String reName) throws IOException {
        String sourceFile= rootDir+dirPath+"/"+fileName;
        String  distFile = rootDir + dirPath+"/"+reName;
        fileSystem.rename(new Path(sourceFile),new Path(distFile));
    }

    public FSDataOutputStream getFileOutPutStream(String filePath) throws IOException {
        String file = rootDir + filePath;
        return fileSystem.create(new Path(file));
    }
}
