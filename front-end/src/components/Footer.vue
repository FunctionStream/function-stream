<template>
  <div class="flex justify-center pt-4 pb-4">
    <a :href="config?.github" class="flex items-center text-blue-500 cursor-pointer" target="_blank"
      ><GithubOutlined class="mr-2" />Function Stream
    </a>
  </div>
</template>
<script setup>
  import config from '@/config/config'
</script>
