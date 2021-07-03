<template>
  <div class="flex items-stretch h-screen">
    <el-scrollbar class="overflow-hidden shadow-lg z-20">
      <slot name="sider" />
    </el-scrollbar>
    <div class="flex flex-col flex-1 overflow-hidden">
      <header class="baselayout-header flex items-center p-4 shadow bg-white">
        <slot name="header" />
      </header>
      <el-scrollbar class="flex-1 baselayout-content-scrollbar">
        <div>
          <slot name="content" />
        </div>
        <slot name="footer" />
      </el-scrollbar>
    </div>
  </div>
</template>

<style>
  .baselayout-header {
    height: 64px;
    overflow: hidden;
    z-index: 10;
  }
  .baselayout-content-scrollbar .el-scrollbar__view {
    min-height: 95%;
    display: flex;
    /* width: 100%; */
    flex-direction: column;
    justify-content: space-between;
  }
</style>
