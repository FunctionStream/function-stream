import axios from 'axios'
import { ElNotification } from 'element-plus'
import { VueAxios } from './axios'
import nProgress from 'nprogress'
import 'nprogress/nprogress.css'

// 创建 axios 实例
const request = axios.create({
  // API 请求的默认前缀
  baseURL: import.meta.env.VITE_APP_API_BASE_URL,
  timeout: 6000 // 请求超时时间
})

// nProgress.configure({ showSpinner: false })

// 异常拦截处理器
const errorHandler = (error) => {
  console.error(error)
  if (error.response) {
    const data = error.response.data
    if (error.response.status === 403) {
      ElNotification.error({
        title: 'Forbidden',
        message: data.message
      })
    }
    if (error.response.status === 401) {
      ElNotification.error({
        title: 'Unauthorized',
        message: 'Authorization verification failed'
      })
    }
    if (error.response.status === 404) {
      ElNotification.error({
        title: '404 Not Found'
      })
    }
  }
  nProgress.done()
  return Promise.reject(error)
}

// request interceptor
request.interceptors.request.use((config) => {
  nProgress.start()
  // const token = storage.get(ACCESS_TOKEN)
  // 如果 token 存在
  // 让每个请求携带自定义 token 请根据实际情况自行修改
  // if (token) {
  //   config.headers['Access-Token'] = token
  // }
  return config
}, errorHandler)

// response interceptor
request.interceptors.response.use((response) => {
  nProgress.done()
  return response.data
}, errorHandler)

const installer = {
  vm: {},
  install(Vue) {
    Vue.use(VueAxios, request)
  }
}

export default request

export { installer as VueAxios, request as axios }
