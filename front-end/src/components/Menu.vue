<template>
  <component
    :is="item.children ? 'ElSubmenu' : 'ElMenuItem'"
    v-for="item in menus"
    :key="item.path"
    :index="item.path"
    :route="item"
  >
    <i v-if="!item.children" :class="item.meta.icon"></i>
    <template #title>
      <i v-if="item.children" :class="item.meta.icon"></i>
      <span>{{ $t(item.name) }}</span>
    </template>
    <SubMenu :menus="item.children" />
  </component>
</template>

<script>
  import { ElMenuItem } from 'element-plus'

  export default {
    name: 'SubMenu',
    components: {
      ElMenuItem
    },
    props: {
      menus: {
        type: Array,
        default: function () {
          return []
        }
      }
    }
  }
</script>
