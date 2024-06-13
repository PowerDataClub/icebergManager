import request from '@/utils/request'

export function catalogList(data) {
  return request({
    url: '/iceberg/catalog/list',
    method: 'post',
    data
  })
}
export function catalogAdd(data) {
  return request({
    url: '/iceberg/catalog/add',
    method: 'post',
    data
  })
}
export function catalogModify(data) {
  return request({
    url: '/iceberg/catalog/modify',
    method: 'post',
    data
  })
}
export function catalogDelete(data) {
  return request({
    url: '/iceberg/catalog/delete',
    method: 'post',
    data
  })
}


// 配置文件
export function getHiveCatalogFiles(catalogId) {
  return request({
    url: `/iceberg/catalog/getHiveCatalogFiles/${catalogId}`,
    method: 'get'
  })
}
export function deleteHiveCatalogFiles(catalogId,file) {
  return request({
    url: `/iceberg/catalog/deleteHiveCatalogFiles/${catalogId}/${file}`,
    method: 'post'
  })
}
export function upLoadHiveCatalogFile(catalogId,data) {
  return request({
    url: `/iceberg/catalog/upLoadHiveCatalogFile/${catalogId}`,
    method: 'post',
    data
  })
}
export function downloadFile(catalogId,file) {
  return request({
    url: `/iceberg/catalog/downloadFile/${catalogId}/${file}`,
    method: 'get',
  })
}


