<template>
  <div>
    <div class="wrapper"><div ref="nodeEditor" class="node-editor"></div></div>
    <canvas id="canvasOutput"></canvas>
  </div>
</template>

<script>
  import { Socket, NodeEditor, Control, Output, Input, Component, Engine } from 'rete'
  import ConnectionPlugin from 'rete-connection-plugin'
  import VueRenderPlugin from 'rete-vue-render-plugin'
  // FIXME throw an error when import ContextMenuPlugin
  // import { ContextMenuPlugin } from 'rete-context-menu-plugin'
  import AreaPlugin from 'rete-area-plugin'
  import VueNumControl from './NumControl.vue'
  import { defineComponent, onMounted, reactive, ref } from '@vue/runtime-core'
  export default defineComponent({
    setup(props) {
      let editor = reactive({})
      let engine = reactive({})
      const nodeEditor = ref(null)

      onMounted(async () => {
        const numSocket = new Socket('Number value')

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
        const components = [new NumComponent(), new AddComponent()]

        editor = new NodeEditor('fs-flow@0.1.0', nodeEditor.value)
        editor.use(ConnectionPlugin)
        editor.use(VueRenderPlugin)
        // // editor.use(ContextMenuPlugin)
        editor.use(AreaPlugin)

        engine = new Engine('fs-flow@0.1.0')

        components.map((c) => {
          editor.register(c)
          engine.register(c)
        })

        const n1 = await components[0].createNode({ num: 2 })
        const n2 = await components[0].createNode({ num: 0 })
        const add = await components[1].createNode()

        n1.position = [80, 200]
        n2.position = [80, 400]
        add.position = [500, 240]

        editor.addNode(n1)
        editor.addNode(n2)
        editor.addNode(add)

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
</style>
