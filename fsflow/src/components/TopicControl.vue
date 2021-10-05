<template>
<div>

<text>test-setting</text>
<input
  type="topic"
  :readonly="readonly"
  :value="value"
  @input="change($event)"
  @dblclick.stop=""
  @pointerdown.stop=""
  @pointermove.stop=""
/>
</div>
</template>

<script>
export default {
  props: ['readonly', 'emitter', 'ikey', 'getData', 'putData'],
  data() {
    return {
      value: "",
    }
  },
  methods: {
    change(e){
      this.value = e.target.value;
      this.putData(this.iKey, this.value);
    }
  },
  mounted() {
    this.putData('some-setting', this.value);
    this.value = this.getData(this.ikey);
  }
}
</script>
