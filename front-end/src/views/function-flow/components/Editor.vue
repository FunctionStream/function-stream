<template>
  <div id="rete" ref="rete">
    <Detail ref="detail" :nodeName="nodeName" />
    <div class="dock dock-menu"></div>
  </div>
</template>

<script>
  import { defineComponent, onMounted, ref } from '@vue/runtime-core'
  import Rete from 'rete'
  import AreaPlugin from 'rete-area-plugin'
  import ConnectionPlugin from 'rete-connection-plugin'
  import DockPlugin from 'rete-dock-plugin'
  import VueRenderPlugin from 'rete-vue-render-plugin'
  // FIXME Console an error when import and use rete-context-menu-plugin
  // import ContextMenuPlugin from 'rete-context-menu-plugin'
  import { FuncComponent } from '../node-editor/components/funcComponent'
  import { TopicComponent } from '../node-editor/components/topicComponent'
  import Detail from './NodeDetail.vue'

  export default defineComponent({
    name: 'Rete',
    components: { Detail },
    setup() {
      const rete = ref(null)

      // node detail
      const detail = ref(null)
      const nodeName = ref('')

      onMounted(() => {
        init(rete.value)
      })

      // Init node editor
      async function init(container) {
        var components = [new FuncComponent('test-ex'), new FuncComponent('test-ex2'), new TopicComponent()]

        var editor = new Rete.NodeEditor('fs-flow@0.1.0', container)
        editor.use(ConnectionPlugin)
        editor.use(VueRenderPlugin)
        // editor.use(ContextMenuPlugin)
        editor.use(AreaPlugin)
        editor.use(DockPlugin, {
          container: document.querySelector('.dock'),
          itemClass: 'item',
          plugins: [VueRenderPlugin]
        })

        var engine = new Rete.Engine('fs-flow@0.1.0')

        components.map((c) => {
          editor.register(c)
          engine.register(c)
        })

        editor.on('zoom', ({ source }) => {
          return source !== 'dblclick'
        })
        editor.on('rendernode', ({ el, node }) => {
          el.addEventListener('dblclick', () => {
            nodeName.value = node.name
            detail.value.visibilityBinding = true
          })
        })

        editor.view.resize()
        AreaPlugin.zoomAt(editor)

        return editor
      }

      return {
        detail,
        rete,
        nodeName
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
