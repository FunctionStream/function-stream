<template>
  <div id="rete" ref="rete">
    <Detail v-model="visibleDrawer" />
    <div class="dock dock-menu"></div>
  </div>
</template>

<script>
  import AreaPlugin from 'rete-area-plugin'
  import ConnectionPlugin from 'rete-connection-plugin'
  import DockPlugin from 'rete-dock-plugin'
  import Detail from './Detail.vue'
  import Rete from 'rete'
  import VueRenderPlugin from 'rete-vue-render-plugin'
  // FIXME Console an error when import and use rete-context-menu-plugin
  // import ContextMenuPlugin from 'rete-context-menu-plugin'
  import { AddComponent } from '../node-editor/components/addComponent'
  import { FuncComponent } from '../node-editor/components/funcComponent'
  import { NumComponent } from '../node-editor/components/numComponent'
  import { TopicComponent } from '../node-editor/components/topicComponent'
  import { defineComponent, onMounted, ref } from '@vue/runtime-core'

  export default defineComponent({
    name: 'Rete',
    components: { Detail },
    setup() {
      const rete = ref(null)
      const visibleDrawer = ref(false)

      onMounted(() => {
        init(rete.value)
      })

      async function init(container) {
        var components = [new NumComponent(), new AddComponent(), new FuncComponent(), new TopicComponent()]

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

        // var n1 = await components[0].createNode({ numb: 22 })
        // var n2 = await components[0].createNode({ numb: 33 })
        // var add = await components[1].createNode()
        // const topic = await components[3].createNode()

        // n1.position = [80, 200]
        // n2.position = [80, 400]
        // add.position = [500, 240]
        // topic.position = [80, 200]

        // editor.addNode(n1)
        // editor.addNode(n2)
        // editor.addNode(add)
        // editor.addNode(topic)

        // editor.connect(n1.outputs.get('num'), add.inputs.get('num'))
        // editor.connect(n2.outputs.get('num'), add.inputs.get('num2'))

        editor.on('process nodecreated noderemoved connectioncreated connectionremoved', async () => {
          await engine.abort()
          await engine.process(editor.toJSON())
        })
        editor.on('zoom', ({ source }) => {
          return source !== 'dblclick'
        })
        editor.on('rendernode', ({ el, node }) => {
          el.addEventListener('dblclick', () => {
            console.log('on')
            visibleDrawer.value = true
          })
        })

        editor.view.resize()
        AreaPlugin.zoomAt(editor)
        editor.trigger('process')

        return editor
      }

      return {
        rete,
        visibleDrawer
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
