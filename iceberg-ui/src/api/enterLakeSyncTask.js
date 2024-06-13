import request from '@/utils/request'

export function syncTaskList(data) {
  return request({
    url: '/iceberg/syncTask/list',
    method: 'post',
    data
  })
}
export function syncTaskAdd(data) {
  return request({
    url: '/iceberg/syncTask/add',
    method: 'post',
    data
  })
}
export function syncTaskModify(data) {
  return request({
    url: '/iceberg/syncTask/modify',
    method: 'post',
    data
  })
}
export function stopKafkaTask(data) {
  return request({
    url: '/iceberg/syncTask/stopKafkaTask',
    method: 'post',
    data
  })
}
export function batchAddHiveTask(data) {
  return request({
    url: '/iceberg/syncTask/batchAddHiveTask',
    method: 'post',
    data
  })
}

export function batchAddMySqlTask(data) {
  return request({
    url: '/iceberg/syncTask/batchAddMySqlTask',
    method: 'post',
    data
  })
}
