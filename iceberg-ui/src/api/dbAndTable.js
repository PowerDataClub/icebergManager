import request from '@/utils/request'

export function databaseList(data) {
  return request({
    url: '/iceberg/database/list',
    method: 'post',
    data
  })
}
export function databaseAdd(data) {
  return request({
    url: '/iceberg/database/add',
    method: 'post',
    data
  })
}
export function databaseDelete(data) {
  return request({
    url: '/iceberg/database/delete',
    method: 'post',
    data
  })
}
export function tableList(data) {
  return request({
    url: '/iceberg/table/list2',
    method: 'post',
    data
  })
}
// export function tableList(data) {
//   return request({
//     url: '/iceberg/table/list',
//     method: 'post',
//     data
//   })
// }
export function tableCreate(data) {
  return request({
    url: '/iceberg/table/createTable',
    method: 'post',
    data
  })
}
export function queryTableInfo(data) {
  return request({
    url: '/iceberg/table/queryTableInfo',
    method: 'post',
    data
  })
}

export function updateTable(data) {
  return request({
    url: '/iceberg/table/updateTable',
    method: 'post',
    data
  })
}
export function tableDelete(data) {
  return request({
    url: '/iceberg/table/delete',
    method: 'post',
    data
  })
}
export function tableMetrics(data) {
  return request({
    url: '/iceberg/table/metrics',
    method: 'post',
    data
  })
}
export function tableColumns(data) {
  return request({
    url: '/iceberg/table/columns',
    method: 'post',
    data
  })
}
export function tablePartition(data) {
  return request({
    url: '/iceberg/table/partition',
    method: 'post',
    data
  })
}
// 快照相关
export function tableSnapshot(data) {
  return request({
    url: '/iceberg/table/snapshot',
    method: 'post',
    data
  })
}
export function setCurrentSnapshot(data) {
  return request({
    url: '/iceberg/table/setCurrentSnapshot',
    method: 'post',
    data
  })
}
export function rollbackSnapshot(data) {
  return request({
    url: '/iceberg/table/rollbackSnapshot',
    method: 'post',
    data
  })
}
export function cherryPickSnapshot(data) {
  return request({
    url: '/iceberg/table/cherryPickSnapshot',
    method: 'post',
    data
  })
}

// 数据相关
export function getTableData(data) {
  return request({
    url: '/iceberg/table/getData',
    method: 'post',
    data
  })
}
export function getDataBySnapshotId(data) {
  return request({
    url: '/iceberg/table/getDataBySnapshotId',
    method: 'post',
    data
  })
}
export function getDataByTime(data) {
  return request({
    url: '/iceberg/table/getDataByTime',
    method: 'post',
    data
  })
}
export function addTableData(data) {
  return request({
    url: '/iceberg/table/addData',
    method: 'post',
    data
  })
}
export function updateTableData(data) {
  return request({
    url: '/iceberg/table/updateData',
    method: 'post',
    data
  })
}

export function delTableData(data) {
  return request({
    url: '/iceberg/table/deleteData',
    method: 'post',
    data
  })
}
export function importTableData(data) {
  return request({
    url: '/iceberg/table/importData',
    method: 'post',
    data
  })
}

export function getMetadataFiles(data) {
  return request({
    url: '/iceberg/table/getMetadataFiles',
    method: 'post',
    data
  })
}
export function getMetadata(data) {
  return request({
    url: '/iceberg/table/getMetadata',
    method: 'post',
    data
  })
}

export function mergeSmallFile(data) {
  return request({
    url: '/iceberg/table/mergeSmallFile',
    method: 'post',
    data
  })
}

// 复制表
export function copyTable(data) {
  return request({
    url: '/iceberg/sql/copyTable',
    method: 'post',
    data
  })
}

// 导出表
export function tableDataToFile(data) {
  return request({
    url: '/iceberg/table/tableDataToFile',
    method: 'post',
    data
  })
}
export function fileToTableData(data) {
  return request({
    url: '/iceberg/table/fileToTableData',
    method: 'post',
    data
  })
}
// 清空表
export function  clearTableData(data) {
  return request({
    url: '/iceberg/table/clearTableData',
    method: 'post',
    data
  })
}




