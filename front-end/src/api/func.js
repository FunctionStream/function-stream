import request from '@/utils/request'

const { post, get, put } = request

export const funcApi = {
  list: '/admin/v3/functions/public/default',
  create: ({ funcName }) => `/admin/v3/functions/public/default/${funcName}`,
  update: ({ funcName }) => `/admin/v3/functions/public/default/${funcName}`,
  info: ({ funcName }) => `/admin/v3/functions/public/default/${funcName}`,
  stats: ({ funcName }) => `/admin/v3/functions/public/default/${funcName}/stats`,
  status: ({ funcName }) => `/admin/v3/functions/public/default/${funcName}/status`,
  trigger: ({ funcName }) => `/admin/v3/functions/public/default/${funcName}/trigger`,
  delete: ({ funcName }) => `/admin/v3/functions/public/default/${funcName}`,
  stop: ({ funcName }) => `/admin/v3/functions/public/default/${funcName}/stop`,
  start: ({ funcName }) => `/admin/v3/functions/public/default/${funcName}/start`
}

export function getList() {
  return get(funcApi.list)
}

export function create(funcName, data) {
  return post(funcApi.create({ funcName }), data, { headers: { 'Content-Type': 'multipart/form-data' } })
}

export function update(funcName, data) {
  return put(funcApi.update({ funcName }), data, { headers: { 'Content-Type': 'multipart/form-data' } })
}

export function getInfo(funcName) {
  return get(funcApi.info({ funcName }))
}

export function getStats(funcName) {
  return get(funcApi.stats({ funcName }))
}

export function getStatus(funcName) {
  return get(funcApi.status({ funcName }))
}

export function triggerFunc(funcName, data) {
  return post(funcApi.trigger({ funcName }), data, { headers: { 'Content-Type': 'multipart/form-data' } })
}

export function deleteFunc(funcName) {
  return request({
    url: funcApi.delete({ funcName }),
    method: 'delete'
  })
}

export function startFunc(funcName) {
  return post(funcApi.start({ funcName }))
}

export function stopFunc(funcName) {
  return post(funcApi.stop({ funcName }))
}
