import { Component, Input, Output } from 'rete'
import { NumControl } from '../controls/numControl'
import { NumSocket } from '../sockets/sockets'

export class AddComponent extends Component {
  constructor() {
    super('Add')
  }

  builder(node) {
    var inp1 = new Input('num', 'Number', NumSocket)
    var inp2 = new Input('num2', 'Number2', NumSocket)
    var out = new Output('res', 'Number', NumSocket)

    inp1.addControl(new NumControl(this.editor, 'num', false))
    inp2.addControl(new NumControl(this.editor, 'num2', false))

    return node.addInput(inp1).addInput(inp2).addControl(new NumControl(this.editor, 'preview', true)).addOutput(out)
  }

  worker(node, inputs, outputs) {
    const n1 = inputs['num'].length ? inputs['num'][0] : node.data.num1
    const n2 = inputs['num2'].length ? inputs['num2'][0] : node.data.num2
    const sum = n1 + n2

    this.editor?.nodes
      .find((n) => n.id == node.id)
      .controls.get('preview')
      .setValue(sum)
    outputs['res'] = sum
  }
}
