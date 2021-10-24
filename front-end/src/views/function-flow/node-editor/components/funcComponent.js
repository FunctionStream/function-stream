import { Component, Input, Output } from 'rete'
import { TopicControl } from '../controls/topicControl.js'
import { TopicSocket } from '../sockets/sockets'

export class FuncComponent extends Component {
  constructor() {
    super('test-func')
    this.contextMenuName = 'Add my comp'
  }
  builder(node) {
    const inp1 = new Input('in', 'Input', TopicSocket)
    const out = new Output('out', 'Output', TopicSocket)
    const log = new Output('log', 'Log', TopicSocket)
    const topicControl = new TopicControl(this.editor, 'preview', false)
    return node.addInput(inp1).addOutput(log).addOutput(out).addControl(topicControl)
  }
  rename(component) {
    return component.contextMenuName || component.name
  }
  worker(node, inputs, outputs) {}
}
