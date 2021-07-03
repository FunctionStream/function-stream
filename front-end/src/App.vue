<template>
  <router-view v-slot="{ Component, route }">
    <transition :name="route.meta.transition || 'el-fade-in'" mode="out-in" :appear="true">
      <keep-alive>
        <component :is="Component" :key="route.meta.usePathKey ? route.path : undefined" />
      </keep-alive>
    </transition>
  </router-view>
</template>
