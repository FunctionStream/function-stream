<template>
  <div v-show="visible" id="rete" ref="rete">
    <div class="dock dock-menu"></div>
  </div>
</template>

<script>
  import { defineComponent, onMounted, ref } from '@vue/runtime-core'
  import init from '../node-editor'

  export default defineComponent({
    name: 'Rete',
    setup() {
      const rete = ref(null)
      const visible = ref(true)

      onMounted(() => {
        init(rete.value)
      })

      return {
        rete,
        visible
      }
    }
  })
</script>

<style>
  #rete {
    width: 100%;
    height: 1000px;
  }

  .node .control input,
  .node .input-control input {
    width: 140px;
  }

  select,
  input {
    width: 100%;
    border-radius: 30px;
    background-color: white;
    padding: 2px 6px;
    border: 1px solid #999;
    font-size: 110%;
    width: 170px;
  }

  .dock {
    height: 100vh;
    width: 20vw;
  }
  .dock-menu {
    display: flex;
    position: absolute;
    top: 80%;
    width: 100%;
    transform: scale(0.6);
    transform-origin: 0 0;
    transition: 0.5s;
    opacity: 0.7;
    background: linear-gradient(to top, white 90%, transparent 100%);
  }
  .dock-menu:hover {
    opacity: 1;
  }
  .dock-menu .dock-item {
    margin: 8em;
  }
  .dock {
    height: 100px;
    overflow-x: auto;
    overflow-y: hidden;
    white-space: nowrap;
  }
  .dock-item {
    display: inline-block;
    vertical-align: top;
    transform: scale(0.8);
    transform-origin: 50% 0;
  }
</style>
