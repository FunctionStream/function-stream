import request from '@/utils/request'

const { post, get } = request;

export const funcApi = {
  list: '/functions/public/default',
  create: '/functions/public/default/myfunc',
  info: ({ funcName }) => `/functions/${funcName}/info`,
  stats: ({ funcName }) => `/functions/${funcName}/stats`,
  status: ({ funcName }) => `/functions/${funcName}/status`,
  trigger: ({ funcName }) => `/functions/${funcName}/trigger`,
  deleteFunc: ({ funcName }) => `/functions/${funcName}/delete`,
}

export function getList() {
  return get(funcApi.list);
}

export function createFunc(data) {
  return post(funcApi.create, { data });
}

export function getInfo(funcName) {
  return get(funcApi.info({ funcName }));
}

export function getStats(funcName) {
  return get(funcApi.stats({ funcName }));
}

export function getStatus(funcName) {
  return get(funcApi.status({ funcName }));
}

export function triggerFunc(funcName, data) {
  return post(funcApi.trigger({ funcName }), { data });
}

export function deleteFunc(funcName) {
  return post(funcApi.deleteFunc({ funcName }));
}



