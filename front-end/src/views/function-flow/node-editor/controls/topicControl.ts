import Rete from 'rete'
import VueTopicControl from './TopicControl.vue'

export class TopicControl extends Rete.Control {
  VueContext: { value: undefined | Number }
  props: { emitter: Object; ikey: String; readonly: Boolean }
  component: any
  constructor(emitter, key, readonly) {
    super(key)
    this.VueContext = { value: undefined }
    this.component = VueTopicControl
    this.props = { emitter, ikey: key, readonly }
  }
  setValue(val) {
    this.VueContext.value = val
  }
}
