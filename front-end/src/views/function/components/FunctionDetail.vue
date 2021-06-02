<template>
  <a-drawer :visible="visible"
            :width="810"
            @close="onClose">
    <!-- function Info -->
    <a-descriptions title="Function Info"
                    bordered>
      <a-descriptions-item label="Name">
        {{currentFuncionInfo.name}}
      </a-descriptions-item>
      <a-descriptions-item label="Classname"
                           :span="2">
        {{currentFuncionInfo.className}}
      </a-descriptions-item>
      <a-descriptions-item label="Status">
        <a-badge status="processing"
                 text="Running" />
      </a-descriptions-item>
      <a-descriptions-item label="Runtime"
                           :span="2">
        {{currentFuncionInfo.runtime}}
      </a-descriptions-item>
      <a-descriptions-item label="Input"
                           :span="3">
        {{currentFuncionInfo.input}}
      </a-descriptions-item>
      <a-descriptions-item label="Output"
                           :span="3">
        {{currentFuncionInfo.output}}
      </a-descriptions-item>
      <a-descriptions-item label="Resources"
                           :span="3">
        <b>CPU</b>: {{currentFuncionInfo.resources && currentFuncionInfo.resources.cpu || 0}}
        <br />
        <b>RAM</b>: {{currentFuncionInfo.resources && currentFuncionInfo.resources.ram || 0}}
        <br />
        <b>DISK</b>: {{currentFuncionInfo.resources && currentFuncionInfo.resources.disk || 0}}
      </a-descriptions-item>
      <a-descriptions-item label="Stats"
                           :span="3">
        <b>Received</b>: {{currentFuncionInfo.receivedTotal || 0}}
        <br />
        <b>Processed Successfully</b>: {{currentFuncionInfo.processedSuccessfullyTotal || 0}}
        <br />
        <b>System Exceptions</b>: {{currentFuncionInfo.systemExceptionsTotal || 0}}
        <br />
        <b>Avg Process Latency</b>: {{currentFuncionInfo.avgProcessLatency || 0}}
      </a-descriptions-item>
    </a-descriptions>
  </a-drawer>
</template>
<script>
export default {
  props: {
    visible: {
      type: Boolean,
      default: false,
    },
    currentFuncionInfo: {
      type: Object,
      default: () => { },
    }
  },
  methods: {
    onClose() {
      // console.log(this.$parent.testname)
      console.log(this.$parent.currentFuncionInfo)
      this.$parent.currentFuncionInfo = {};
      this.$parent.closeDetail();
    },
  }
}
</script>

