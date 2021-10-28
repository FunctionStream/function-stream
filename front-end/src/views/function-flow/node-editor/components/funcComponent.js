import { Component, Input, Output } from 'rete'
import { TopicSocket } from '../sockets/sockets'

export class FuncComponent extends Component {
  constructor(props) {
    super(props)
    this.contextMenuName = 'Add my comp'
  }
  builder(node) {
    const inp1 = new Input('in', 'Input', TopicSocket)
    const out = new Output('out', 'Output', TopicSocket)
    return node.addInput(inp1).addOutput(out)
  }
  rename(component) {
    return component.contextMenuName || component.name
  }
  worker(node, inputs, outputs) {}
}
