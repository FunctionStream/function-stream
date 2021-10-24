import Rete from 'rete'
import AreaPlugin from 'rete-area-plugin'
import ConnectionPlugin from 'rete-connection-plugin'
import DockPlugin from 'rete-dock-plugin'
import VueRenderPlugin from 'rete-vue-render-plugin'
// import ContextMenuPlugin from 'rete-context-menu-plugin'
import { AddComponent } from './components/addComponent'
import { FuncComponent } from './components/funcComponent'
import { NumComponent } from './components/numComponent'
import { TopicComponent } from './components/topicComponent'

export default async function (container) {
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
  const topic = await components[3].createNode()

  // n1.position = [80, 200]
  // n2.position = [80, 400]
  // add.position = [500, 240]
  topic.position = [0, 20]

  // editor.addNode(n1)
  // editor.addNode(n2)
  // editor.addNode(add)
  editor.addNode(topic)

  // editor.connect(n1.outputs.get('num'), add.inputs.get('num'))
  // editor.connect(n2.outputs.get('num'), add.inputs.get('num2'))

  editor.on('process nodecreated noderemoved connectioncreated connectionremoved', async () => {
    await engine.abort()
    await engine.process(editor.toJSON())
  })

  editor.view.resize()
  AreaPlugin.zoomAt(editor)
  editor.trigger('process')
}
