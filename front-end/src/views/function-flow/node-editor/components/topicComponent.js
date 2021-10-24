import { Component, Input, Output } from 'rete'
import { TopicControl } from '../controls/topicControl.js'
import { TopicSocket } from '../sockets/sockets'

export class TopicComponent extends Component {
  constructor() {
    super('topic component')
  }

  builder(node) {
    const inp = new Input('in', 'Input', TopicSocket)
    const out = new Output('out', 'Output', TopicSocket)
    const topicControl = new TopicControl(this.editor, 'preview', false)

    return node.addInput(inp).addOutput(out).addControl(topicControl)
  }
  rename(component) {
    return component.contextMenuName || component.name
  }
  worker(node, inputs, outputs) {}
}
