import Rete from 'rete'
// FIXME console an error when import and use CustomNode
// import CustomNode from '../../components/Node.vue'
import { NumControl } from '../controls/numControl.js'
import { NumSocket } from '../sockets/sockets'

export class NumComponent extends Rete.Component {
  constructor() {
    super('Number')
    // this.data.component = CustomNode
    // this.data.render = 'vue'
  }

  builder(node) {
    var out1 = new Rete.Output('num', 'Number', NumSocket)
    return node.addControl(new NumControl(this.editor, 'numb')).addOutput(out1)
  }

  worker(node, inputs, outputs) {
    outputs['num'] = node.data.numb
  }
}
