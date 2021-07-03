<template>
  <div class="p-4 bg-white select-none">
    <el-breadcrumb separator="/">
      <el-breadcrumb-item v-for="router in routes" :key="router.path" :to="router">
        {{ $t(router.name) }}
      </el-breadcrumb-item>
    </el-breadcrumb>
    <div class="flex justify-between items-center mt-3">
      <span class="text-xl font-bold">{{ $t(router.name) }}</span>
      <div>
        <slot name="extra" />
      </div>
    </div>
  </div>
  <div class="m-4 shadow-sm bg-white p-4">
    <slot />
  </div>
</template>

<script>
  import { useRoute } from 'vue-router'
  export default {
    setup() {
      const router = useRoute()
      const routes = router.matched
      return {
        router,
        routes
      }
    }
  }
</script>
