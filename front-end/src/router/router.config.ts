import BaseLayout from '@/layouts/BaseLayout.vue'

// RouteView
export const asyncRouterMap = [
  {
    path: '/',
    name: 'menu.Home',
    component: BaseLayout,
    meta: { title: 'Home' },
    redirect: '/stream-flow-diagram',
    children: [
      {
        path: '/function',
        name: 'menu.function',
        meta: { title: 'function', icon: 'el-icon-s-grid' },
        component: () => import('@/views/function/index.vue')
      },
      {
        path: '/stream-flow-diagram',
        name: '数据流',
        meta: { title: 'stream-flow-diagram', icon: 'el-icon-s-grid' },
        component: () => import('@/views/data-flow-diagram/X6_index.vue')
      }
    ]
  },
  {
    path: '/:pathMatch(.*)*',
    component: () => import('@/views/exception/404.vue')
  }
]
