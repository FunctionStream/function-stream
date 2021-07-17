<template>
  <div class="w-full flex justify-between items-center select-none">
    <div>
      <span @click="onChangMenuCollapse">
        <i class="text-xl hover:text-blue-500 mr-2" :class="isCollapse ? 'el-icon-s-unfold' : 'el-icon-s-fold'" />
      </span>
      <i class="el-icon-refresh-right text-xl hover:text-blue-500" @click="onRefresh" />
    </div>
    <div>
      <!-- lang -->
      <el-dropdown trigger="click" class="mr-4" @command="onChangeLang">
        <span class="el-dropdown-link">
          {{ LangMap[$i18n.locale] }}
          <i class="el-icon-arrow-down el-icon--right"></i>
        </span>
        <template #dropdown>
          <el-dropdown-menu>
            <el-dropdown-item command="zh-cn">中文</el-dropdown-item>
            <el-dropdown-item command="en">English</el-dropdown-item>
          </el-dropdown-menu>
        </template>
      </el-dropdown>
    </div>
  </div>
</template>
<script>
  import store from 'store'
  import defaultConfig from '@/config/config'
  import { computed } from 'vue'
  import { useStore } from 'vuex'

  export default {
    setup() {
      const store = useStore()
      const LangMap = {
        'zh-cn': '中文',
        en: 'English'
      }
      return {
        LangMap,
        isCollapse: computed(() => store.state.app.sideCollapsed),
        onChangMenuCollapse: () => store.dispatch('app/onChangeSideCollapsed', !store.state.app.sideCollapsed),
        onRefresh: () => store.dispatch('app/onRefresh')
      }
    },
    created() {
      const lang = store.get('lang') || defaultConfig?.defaultLang || 'en'
      this.$i18n.locale = lang
    },
    methods: {
      onChangeLang(lang) {
        this.$i18n.locale = lang
        store.set('lang', lang)
      }
    }
  }
</script>
