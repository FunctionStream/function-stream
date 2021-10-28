import Rete from 'rete'
import VueNumControl from './numControl.vue'

export class NumControl extends Rete.Control {
  vueContext: { value: undefined | Number }
  component: Object
  props: { emitter: Object; ikey: String; readonly: Boolean }
  constructor(emitter, key, readonly) {
    super(key)
    this.vueContext = { value: undefined }
    this.component = VueNumControl
    this.props = { emitter, ikey: key, readonly }
  }

  setValue(val) {
    this.vueContext.value = val
  }
}
