import PageHeaderWrapper from './PageHeaderWrapper.vue'

// 注册自定义常用组件到全局
export default function customComponents(app) {
  app.component('PageHeaderWrapper', PageHeaderWrapper)
}
