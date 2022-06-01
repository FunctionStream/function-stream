<template>
  <div class="node" :class="kebab([selected(), node.name])">
    <div class="title">{{ node.name }}</div>
    <!-- Outputs-->
    <div v-for="output in outputs()" :key="output.key" class="output">
      <div class="output-title">{{ output.name }}</div>
      <Socket v-socket:output="output" type="output" :socket="output.socket"></Socket>
    </div>
    <!-- Controls-->
    <div v-for="control in controls()" :key="control.key" v-control="control" class="control"></div>
    <!-- Inputs-->
    <div v-for="input in inputs()" :key="input.key" class="input">
      <Socket v-socket:input="input" type="input" :socket="input.socket"></Socket>
      <div v-show="!input.showControl()" class="input-title">{{ input.name }}</div>
      <div v-show="input.showControl()" v-control="input.control" class="input-control"></div>
    </div>
  </div>
</template>

<script>
  import VueRender from 'rete-vue-render-plugin'
  import Socket from './Socket.vue'

  export default {
    components: {
      Socket
    },
    mixins: [VueRender.mixin]
  }
</script>

<style>
  .node {
    background: rgba(110, 136, 255, 0.8);
    border: 2px solid #4e58bf;
    border-radius: 0px;
    cursor: pointer;
    min-width: 180px;
    height: auto;
    padding-bottom: 6px;
    box-sizing: content-box;
    position: relative;
    user-select: none;
  }

  .node:hover {
    background: rgba(130, 153, 255, 0.8);
  }

  .node.selected {
    background: #ffd92c;
    border-color: #e3c000;
  }

  .node .title {
    color: white;
    font-family: sans-serif;
    font-size: 18px;
    padding: 8px;
  }

  .node .output {
    text-align: right;
  }

  .node .input {
    text-align: left;
  }

  .node .input-title,
  .node .output-title {
    vertical-align: middle;
    color: white;
    display: inline-block;
    font-family: sans-serif;
    font-size: 14px;
    margin: 6px;
    line-height: 24px;
  }

  .node .input-control {
    z-index: 1;
    width: calc(100% - 36px);
    vertical-align: middle;
    display: inline-block;
  }

  .node .control {
    padding: 6px 18px;
  }
</style>
