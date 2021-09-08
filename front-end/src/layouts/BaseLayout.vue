<template>
  <BaseLayout>
    <template #sider>
      <Sider :title="title" :menus="menus" />
    </template>

    <template #header>
      <Header />
    </template>

    <template #content>
      <router-view v-if="!isRefresh"></router-view>
    </template>

    <template #footer>
      <Footer />
    </template>
  </BaseLayout>
</template>

<script>
  import BaseLayout from '@/components/BaseLayout.vue'
  import Sider from '@/components/Sider.vue'
  import Header from '@/components/Header.vue'
  import Footer from '@/components/Footer.vue'
  import { ref, computed } from 'vue'
  import { asyncRouterMap } from '@/router/router.config'
  import Config from '@/config/config'
  import { useStore } from 'vuex'

  export default {
    components: {
      BaseLayout,
      Sider,
      Header,
      Footer
    },
    setup() {
      const menus = ref([])
      const title = ref(Config.title)
      const store = useStore()
      // menu
      const routes = asyncRouterMap.find((item) => item.path === '/')
      menus.value = (routes && routes?.children) || []

      return {
        title,
        menus,
        isRefresh: computed(() => store.state.app.isRefresh)
      }
    },
    watch: {
      isRefresh() {
        if (!this.isRefresh) return // 避免两次
        this.$nextTick(() => this.$store.dispatch('app/onRefresh', false))
      }
    }
  }
</script>
