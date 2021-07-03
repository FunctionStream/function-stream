<template>
  <el-result title="404" subTitle="抱歉，请求错误"> </el-result>
</template>
<script>
  export default {
    methods: {
      backHome() {
        this.$router.back()
      }
    }
  }
</script>
