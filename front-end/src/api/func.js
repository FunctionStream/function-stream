import request from '@/utils/request'

const { post, get } = request

export const funcApi = {
  list: '/admin/v3/functions/public/default',
  create: '/admin/v3/functions/public/default/myfunc',
  info: ({ funcName }) => `/admin/v3/functions/public/default/${funcName}`,
  stats: ({ funcName }) => `/admin/v3/functions/public/default/${funcName}/stats`,
  status: ({ funcName }) => `/admin/v3/functions/public/default/${funcName}/status`,
  trigger: ({ funcName }) => `/admin/v3/functions/public/default/${funcName}/trigger`,
  delete: ({ funcName }) => `/admin/v3/functions/public/default/${funcName}/delete`,
  stop: ({ funcName }) => `/admin/v3/functions/public/default/${funcName}/stop`,
  start: ({ funcName }) => `/admin/v3/functions/public/default/${funcName}/start`
}

export function getList () {
  return get(funcApi.list)
}

export function createFunc (data) {
  return post(funcApi.create, { data })
}

export function getInfo (funcName) {
  return get(funcApi.info({ funcName }))
}

export function getStats (funcName) {
  return get(funcApi.stats({ funcName }))
}

export function getStatus (funcName) {
  return get(funcApi.status({ funcName }))
}

export function triggerFunc (funcName, data) {
  return post(funcApi.trigger({ funcName }), { data })
}

export function deleteFunc (funcName) {
  return request({
    url: funcApi.delete({ funcName }),
    method: 'delete'
  })
}

export function startFunc (funcName) {
  return post(funcApi.start({ funcName }))
}

export function stopFunc (funcName) {
  return post(funcApi.stop({ funcName }))
}
