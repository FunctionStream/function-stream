import Rete from 'rete'
import VueTopicControl from './TopicControl.vue'

export class TopicControl extends Rete.Control {
  constructor(emitter, key, readonly) {
    super(key)
    this.component = VueTopicControl
    this.props = { emitter, ikey: key, readonly }
  }
  setValue(val) {
    this.VueContext.value = val
  }
}
