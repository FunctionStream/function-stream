/* eslint-disable */
import router from './router'
import store from './store'
import storage from 'store'
import NProgress from 'nprogress' // progress bar
import '@/components/NProgress/nprogress.less' // progress bar custom style
import notification from 'ant-design-vue/es/notification'
import { setDocumentTitle, domTitle } from '@/utils/domUtil'
import { ACCESS_TOKEN, LOGIN_INFO, REMEMBER_ME, MANUAL_EXIT } from '@/store/mutation-types'
import { i18nRender } from '@/locales'
import { timeFix } from '@/utils/util.js'

NProgress.configure({ showSpinner: false }) // NProgress Configuration

const allowList = ['login', 'register', 'registerResult'] // no redirect allowList
const loginRoutePath = '/user/login'
const homePagePath = '/'
const setWindowTitle = (meta) => {
  const { title } = meta
  typeof title !== 'undefined' && setDocumentTitle(`${ i18nRender(title) } - ${ domTitle }`)
}

router.beforeEach((to, from, next) => {
  NProgress.start() // start progress bar
  to.meta && setWindowTitle(to.meta)
  /* has token */
  if (storage.get(ACCESS_TOKEN)) {
    if (to.path === loginRoutePath) {
      next({ path: homePagePath })
      NProgress.done()
    } else {
      // check login user.roles is null
      if (store.getters.roles.length === 0) {
        // request login userInfo
        store
          .dispatch('GetInfo')
          .then(res => {
            const roles = res.result && res.result.role
            // generate dynamic router
            store.dispatch('GenerateRoutes', { roles }).then(() => {
              // 根据roles权限生成可访问的路由表
              // 动态添加可访问路由表
              router.addRoutes(store.getters.addRouters)
              // 请求带有 redirect 重定向时，登录自动重定向到该地址
              const redirect = decodeURIComponent(from.query.redirect || to.path)
              if (to.path === redirect) {
                // set the replace: true so the navigation will not leave a history record
                next({ ...to, replace: true })
              } else {
                // 跳转到目的路由
                next({ path: redirect })
              }
            })
          })
          .catch(() => {
            notification.error({
              message: i18nRender('user.login.login-exception'),
              description: i18nRender('user.login.login-exception')
            })
            // 失败时，获取用户信息失败时，调用登出，来清空历史保留信息
            store.dispatch('Logout').then(() => {
              next({ path: loginRoutePath, query: { redirect: to.fullPath } })
            })
          })
      } else {
        next()
      }
    }
  } else {
    if (allowList.includes(to.name)) {
      if (to.name === 'login' && storage.get(REMEMBER_ME) === 'true' && storage.get(MANUAL_EXIT) !== 'true') {
        // todo winnd: 这部分是自动登录, 具体实现要和后台协商，因为前端不应该明文保存密码 ① 前后端协商token什么时候失效 ② 开关自动登录时向后台发送通知请求
        try {
          const loginParams = JSON.parse(storage.get(LOGIN_INFO))

          store
            .dispatch('Login', loginParams)
            .then(() => {
              next({ path: homePagePath })
              notification.success({
                message: '欢迎',
                description: `${ timeFix() }，欢迎回来`
              })
            })
            .catch((err) => {
              notification.error({
                message: '错误',
                description: ((err.response || {}).data || {}).message || '请求出现错误，请稍后再试',
                duration: 4
              })
            })
        } catch (err) {
          console.log(err)
          storage.set(LOGIN_INFO, '')
          storage.set(REMEMBER_ME, 'false')
          notification.error({ message: '错误', description: '自动登录已失效，请重新登录', duration: 4 })
          next({ path: loginRoutePath, query: { redirect: to.fullPath } })
          NProgress.done()
        }
      } else {
        next()
      }
    } else {
      next({ path: loginRoutePath, query: { redirect: to.fullPath } })
      NProgress.done() // if current page is login will not trigger afterEach hook, so manually handle it
    }
  }
})

router.afterEach(() => {
  NProgress.done() // finish progress bar
})
