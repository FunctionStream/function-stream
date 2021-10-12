<template>
  <input
    type="number"
    :readonly="readonly"
    :value="value"
    @input="change($event)"
    @dblclick.stop=""
    @pointerdown.stop=""
    @pointermove.stop=""
  />
</template>

<script>
  import { ref } from '@vue/reactivity'
  import { onMounted } from '@vue/runtime-core'
  export default {
    props: {
      emitter: {
        type: Object,
        default: null
      },
      ikey: {
        type: String,
        default: null
      },
      getData: {
        type: Function,
        default: () => {}
      },
      putData: {
        type: Function,
        default: () => {}
      },
      readonly: Boolean
    },
    setup(props) {
      const value = ref('')
      onMounted(() => {
        value.value = props.getData(props.ikey)
      })

      function change(e) {
        value.value = +e.target.value
        update()
      }

      function update() {
        if (props.ikey) {
          props.putData(props.ikey, value.value)
        }
        props.emitter.trigger('process')
      }

      return {
        value,
        change
      }
    }
  }
</script>
