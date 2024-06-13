import request from '@/utils/request'

// sql客户端
export function sqlExecute(data) {
  return request({
    url: '/iceberg/sql/execute',
    method: 'post',
    data
  })
}

// sql管理
export function sqlLogList(data) {
  return request({
    url: '/iceberg/sqlLog/list',
    method: 'post',
    data
  })
}
export function sqlLogAdd(data) {
  return request({
    url: '/iceberg/sqlLog/add',
    method: 'post',
    data
  })
}
export function sqlLogModify(data) {
  return request({
    url: '/iceberg/sqlLog/modify',
    method: 'post',
    data
  })
}
export function sqlLogDelete(data) {
  return request({
    url: '/iceberg/sqlLog/delete',
    method: 'post',
    data
  })
}

// 执行历史
export function sqlLogHistory(data) {
  return request({
    url: '/iceberg/sqlLog/history',
    method: 'post',
    data
  })
}



