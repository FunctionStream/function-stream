import request from '@/utils/request'

const { post, get } = request;

export const funcApi = {
  list: '/function/list',
  create: '/function/create',
  info: ({ funcName }) => `/function/${funcName}/info`,
  stats: ({ funcName }) => `/function/${funcName}/stats`,
  status: ({ funcName }) => `/function/${funcName}/status`,
  trigger: ({ funcName }) => `/function/${funcName}/trigger`,
  deleteFunc: ({ funcName }) => `/function/${funcName}/delete`,
  startFunc: ({ funcName }) => `/function/${funcName}/start`,
  stopFunc: ({ funcName }) => `/function/${funcName}/stop`,
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

export function startFunc(funcName) {
  return post(funcApi.startFunc({ funcName }));
}

export function stopFunc(funcName) {
  return post(funcApi.startFunc({ funcName }));
}