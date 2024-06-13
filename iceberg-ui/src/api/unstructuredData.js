import request from '@/utils/request'

export function listFile(data) {
  return request({
    url: '/iceberg/unStructured/list',
    method: 'post',
    data
  })
}

// hdfs图片、视频预览
export function reviewMP4AndPic(params) {
  return request({
    url: `/iceberg/unStructured/reviewMP4AndPic/${params.catalogId}`,
    method: 'get',
    responseType:'blob',
    params
  })
}
// 文档预览
export function catFile(data) {
  return request({
    url: `/iceberg/unStructured/catFile`,
    method: 'post',
    data
  })
}

// 批量上传
export function batchUploadFileToHdfs(data,callback) {
  return request({
    url: `/iceberg/unStructured/batchUploadFileToHdfs`,
    method: 'post',
    data,
    callback
  })
}

// 文件下载
export function fastDownloadHdfsFile(params) {
  return request({
    url: `/iceberg/unStructured/fastDownloadHdfsFile/${params.catalogId}`,
    method: 'get',
    responseType:'blob',
    params
  })
}
// 文件下载
export function fastDownloadHdfsDir(params) {
  return request({
    url: `/iceberg/unStructured/fastDownloadHdfsDir/${params.catalogId}`,
    method: 'get',
    responseType:'blob',
    params
  })
}

// 文件删除
export function checkAndDeleteFile(data) {
  return request({
    url: `/iceberg/unStructured/checkAndDeleteFile`,
    method: 'post',
    data
  })
}
// 文件强制删除
export function batchDeleteFile(data) {
  return request({
    url: `/iceberg/unStructured/batchDeleteFile`,
    method: 'post',
    data
  })
}

// 新建文件夹
export function addDir(data) {
  return request({
    url: `/iceberg/unStructured/addDir`,
    method: 'post',
    data
  })
}
// 剪切、粘贴
export function moveFile(data) {
  return request({
    url: `/iceberg/unStructured/moveFile`,
    method: 'post',
    data
  })
}
// 重命名
export function renameFile(data) {
  return request({
    url: `/iceberg/unStructured/renameFile`,
    method: 'post',
    data
  })
}