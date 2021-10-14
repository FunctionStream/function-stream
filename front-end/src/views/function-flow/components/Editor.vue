<template>
  <div>
    <div class="wrapper">
      <div class="editor">
        <div class="container">
          <div ref="nodeEditor" class="node-editor"></div>
        </div>
        <div class="dock dock-menu"></div>
      </div>
    </div>
    <canvas id="canvasOutput"></canvas>
  </div>
</template>

<script>
  import { Socket, NodeEditor, Control, Output, Input, Component, Engine } from 'rete'
  import ConnectionPlugin from 'rete-connection-plugin'
  import VueRenderPlugin from 'rete-vue-render-plugin'
  // FIXME throw an error when import ContextMenuPlugin
  // import { ContextMenuPlugin } from 'rete-context-menu-plugin'
  import DockPlugin from 'rete-dock-plugin'
  import AreaPlugin from 'rete-area-plugin'
  import { defineComponent, onMounted, reactive, ref } from '@vue/runtime-core'

  import VueNumControl from './NumControl.vue'
  import VueTopicControl from './TopicControl.vue'
  export default defineComponent({
    setup(props) {
      let editor = reactive({})
      let engine = reactive({})
      const nodeEditor = ref(null)

      onMounted(async () => {
        const numSocket = new Socket('Number value')
        const topicSocket = new Socket('Topic value')

        class NumControl extends Control {
          constructor(emitter, key, readonly) {
            super(key)
            this.component = VueNumControl
            this.props = { emitter, ikey: key, readonly }
          }

          setValue(val) {
            this.vueContext.value = val
          }
        }

        class TopicControl extends Control {
          constructor(emitter, key, readonly) {
            super(key)
            this.component = VueTopicControl
            this.props = { emitter, ikey: key, readonly }
          }

          setValue(val) {
            this.VueContext.value = val
          }
        }

        class NumComponent extends Component {
          constructor() {
            super('Number')
          }

          builder(node) {
            const out1 = new Output('num', 'Number', numSocket)
            return node.addControl(new NumControl(editor, 'num')).addOutput(out1)
          }

          worker(node, inputs, outputs) {
            outputs['num'] = node.data.num
          }
        }

        class AddComponent extends Component {
          constructor() {
            super('Add')
          }
          builder(node) {
            const inp1 = new Input('num', 'Number', numSocket)
            const inp2 = new Input('num2', 'Number2', numSocket)
            const out = new Output('num', 'Number', numSocket)

            inp1.addControl(new NumControl(editor, 'num'))
            inp2.addControl(new NumControl(editor, 'num2'))

            return node.addInput(inp1).addInput(inp2).addControl(new NumControl(editor, 'preview', true)).addOutput(out)
          }
          worker(node, inputs, outputs) {
            const n1 = inputs['num'].length ? inputs['num'][0] : node.data.num1
            const n2 = inputs['num2'].length ? inputs['num2'][0] : node.data.num2
            const sum = n1 + n2

            this.editor.nodes
              .find((n) => n.id == node.id)
              .controls.get('preview')
              .setValue(sum)
            outputs['num'] = sum
          }
        }

        class TestFunc extends Component {
          constructor() {
            super('test-func')
            this.contextMenuName = 'Add my comp'
          }

          builder(node) {
            const inp1 = new Input('in', 'Input', topicSocket)
            const out = new Output('out', 'Output', topicSocket)
            const log = new Output('log', 'Log', topicSocket)

            const topicControl = new TopicControl(this.editor, 'preview', false)

            return node.addInput(inp1).addOutput(log).addOutput(out).addControl(topicControl)
          }

          rename(component) {
            return component.contextMenuName || component.name
          }

          worker(node, inputs, outputs) {}
        }

        const components = [new NumComponent(), new AddComponent(), new TestFunc()]

        editor = new NodeEditor('fs-flow@0.1.0', nodeEditor.value)
        editor.use(ConnectionPlugin)
        editor.use(VueRenderPlugin)
        // editor.use(ContextMenuPlugin)
        editor.use(AreaPlugin, {
          background: false,
          snap: false,
          scaleExtent: { min: 1, max: 2 },
          translateExtent: { width: 5000, height: 4000 }
        })
        editor.use(DockPlugin, {
          container: document.querySelector('.dock'),
          itemClass: 'item',
          plugins: [VueRenderPlugin]
        })

        engine = new Engine('fs-flow@0.1.0')

        components.map((c) => {
          editor.register(c)
          engine.register(c)
        })

        const n1 = await components[0].createNode({ num: 2 })
        const n2 = await components[0].createNode({ num: 0 })
        const add = await components[1].createNode()
        // const testFunc = await components[2].createNode()

        n1.position = [80, 200]
        n2.position = [80, 400]
        add.position = [500, 240]

        editor.addNode(n1)
        editor.addNode(n2)
        editor.addNode(add)
        // editor.addNode(testFunc)

        editor.connect(n1.outputs.get('num'), add.inputs.get('num'))
        editor.connect(n2.outputs.get('num'), add.inputs.get('num2'))

        editor.on('process nodecreated noderemoved connectioncreated connectionremoved', async () => {
          console.log('process')
          await engine.abort()
          await engine.process(editor.toJSON())
        })

        editor.view.resize()
        AreaPlugin.zoomAt(editor)
        editor.trigger('process')
      })

      return {
        nodeEditor,
        editor,
        engine
      }
    }
  })
</script>

<style>
  .node-editor {
    text-align: left;
    height: 100vh;
    width: 100vw;
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

  .container {
    flex: 1;
    overflow: hidden;
  }
</style>
