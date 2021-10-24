<template>
  <div>
    <input
      type="topic"
      :readonly="readonly"
      :value="value"
      placeholder="enter name here"
      @input="change($event)"
      @dblclick.stop=""
      @pointerdown.stop=""
      @pointermove.stop=""
    />
  </div>
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
        props.putData('some-setting', value.value)
        value.value = props.getData(props.ikey)
      })

      function change(e) {
        value.value = e.target.value
        props.putData(props.ikey, value.value)
      }

      return {
        value,
        change
      }
    }
  }
</script>
