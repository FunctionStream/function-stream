<template>
  <global-footer class="footer custom-render">
    <template v-slot:links>
      Powered by
      <a href="https://github.com/vueComponent"
         target="_blank">vueComponent
      </a>
    </template>
    <template v-slot:copyright>
      <a href="https://github.com/FunctionStream/function-stream"
         target="_blank">
        <a-icon type="github" />
        Function Stream
      </a>
    </template>
  </global-footer>
</template>

<script>
import { GlobalFooter } from '@ant-design-vue/pro-layout'

export default {
  name: 'ProGlobalFooter',
  components: {
    GlobalFooter
  }
}
</script>
