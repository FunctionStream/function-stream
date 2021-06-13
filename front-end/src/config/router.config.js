// eslint-disable-next-line
import { UserLayout, BasicLayout, } from '@/layouts'

// ↓↓ <view-router />
const RouteView = {
  name: 'RouteView',
  render: h => h('router-view')
}

export const asyncRouterMap = [
  {
    path: '/',
    name: 'index',
    component: BasicLayout,
    meta: { title: 'menu.home' },
    redirect: '/function',
    children: [
      {
        path: '/function',
        name: 'function',
        meta: { title: 'menu.function', icon: 'table', permission: ['user'] },
        component: () => import('@/views/function/index')
      },
      {
        path: '/user-management',
        name: 'user-management',
        component: () => import('@/views/user-management/Index.vue'),
        meta: { title: 'menu.settings.user-management', icon: 'user', permission: ['user', 'manager'] }
      },
      {
        path: '/account',
        component: RouteView,
        redirect: '/account/settings',
        name: 'account',
        meta: { title: 'menu.settings', icon: 'setting', keepAlive: true },
        children: [
          {
            path: '/account/settings',
            name: 'settings',
            component: () => import('@/views/account/settings/Index'),
            meta: { title: 'menu.settings.common', hidden: true, icon: 'bars' },
            redirect: '/account/settings/common',
            hideChildrenInMenu: true,
            children: [
              {
                path: '/account/settings/common',
                name: 'CommonSettings',
                component: () => import('@/views/account/settings/CommonSettings'),
                meta: { title: 'menu.settings.common', hidden: true, icon: 'bars' }
              }
            ]
          }
        ]
      }
    ]
  },
  {
    path: '*',
    redirect: '/404',
    hidden: true
  }
]

/**
 * 基础路由
 * @type { *[] }
 */
export const constantRouterMap = [
  {
    path: '/user',
    component: UserLayout,
    redirect: '/user/login',
    hidden: true,
    children: [
      {
        path: 'login',
        name: 'login',
        component: () => import(/* webpackChunkName: "user" */ '@/views/user/Login')
      }
    ]
  },
  {
    path: '/404',
    name: '404',
    component: () => import(/* webpackChunkName: "fail" */ '@/views/exception/404.vue')
  }
]
