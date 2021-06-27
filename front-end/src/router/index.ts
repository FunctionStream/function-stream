import { createRouter, createWebHistory } from 'vue-router'
import { asyncRouterMap } from '@/router/router.config'

export default createRouter({
  history: createWebHistory(),
  routes: asyncRouterMap
})
