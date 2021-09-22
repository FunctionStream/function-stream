import { createApp } from 'vue/dist/vue.esm-bundler'
import App from './App.vue'
import './style.css'
import './mock'

import ElementPlus from 'element-plus'
import { VueAxios } from '@/utils/request'
import router from '@/router/index'
import antdIcon from './core/antdv_icon'
import i18n from './i18n'
import store from '@/store/index'
import customComponents from '@/components/index'

const app = createApp(App)

app.use(i18n)
app.use(antdIcon)
app.use(router)
app.use(VueAxios)
app.use(ElementPlus, { i18n: i18n.global.t })
app.use(store)
app.use(customComponents)

app.mount('#app')
