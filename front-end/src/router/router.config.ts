import BaseLayout from '@/layouts/BaseLayout.vue'
import { RouterView } from 'vue-router'

// RouteView
export const asyncRouterMap = [
  {
    path: '/',
    name: 'menu.Home',
    component: BaseLayout,
    meta: { title: 'Home' },
    redirect: '/function',
    children: [
      {
        path: '/function',
        name: 'menu.function',
        meta: { title: 'function', icon: 'el-icon-s-grid' },
        component: () => import('@/views/function/index.vue')
      },
      {
        path: '/functionHub',
        name: 'menu.functionHub',
        meta: { title: 'functionHub', icon: 'el-icon-s-grid' },
        component: () => import('@/views/functionHub/index.vue')
      }
    ]
  },
  {
    path: '/:pathMatch(.*)*',
    component: () => import('@/views/exception/404.vue')
  }
]
