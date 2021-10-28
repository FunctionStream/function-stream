import { Component, Input, Output } from 'rete'
// FIXME console an error when import and use CustomNode
// import CustomNode from '../../components/Node.vue'
import { TopicControl } from '../controls/topicControl'
import { TopicSocket } from '../sockets/sockets'

export class TopicComponent extends Component {
  constructor() {
    super('topic component')
  }

  builder(node) {
    const inp = new Input('in', 'Input', TopicSocket)
    const out = new Output('out', 'Output', TopicSocket)
    const topicControl = new TopicControl(this.editor, 'test-ex2', false)

    return node.addInput(inp).addOutput(out).addControl(topicControl)
  }
  rename(component) {
    return component.contextMenuName || component.name
  }
  worker(node, inputs, outputs) {}
}
