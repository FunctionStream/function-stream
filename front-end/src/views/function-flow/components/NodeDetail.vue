<template>
  <el-form ref="nodeDetail" :model="info">
    <el-drawer v-model="visibilityBinding" size="40%">
      <el-descriptions title="Node detail" :column="2" border class="description" style="padding: 0 20px">
        <template #extra>
          <el-button v-show="!editable" type="primary" @click="editable = true">Edit</el-button>
          <span v-show="editable">
            <el-button type="primary" :style="{ marginRight: '16px' }" @click="saveEdit()"> Save </el-button>
            <el-button @click="cancelEdit()"> Cancel </el-button>
          </span>
        </template>
        <el-descriptions-item label="Name" span="2">
          <el-form-item class="editable">
            <el-input v-model="info.name" readonly />
          </el-form-item>
        </el-descriptions-item>
        <el-descriptions-item label="Replicas">
          <el-form-item :class="{ editable: !editable }">
            <el-input v-model="info.replicas" :readonly="!editable" />
          </el-form-item>
        </el-descriptions-item>
        <el-descriptions-item label="MaxReplicas">
          <el-form-item :class="{ editable: !editable }">
            <el-input v-model="info.maxReplicas" :readonly="!editable" />
          </el-form-item>
        </el-descriptions-item>
        <el-descriptions-item label="CPU">
          <el-form-item :class="{ editable: !editable }">
            <el-input v-model="info.resources.requests.cpu" :readonly="!editable" />
          </el-form-item>
        </el-descriptions-item>
        <el-descriptions-item label="Memory">
          <el-form-item :class="{ editable: !editable }">
            <el-input v-model="info.resources.requests.memory" :readonly="!editable" />
          </el-form-item>
        </el-descriptions-item>
        <el-descriptions-item label="Image" span="2">
          <el-form-item class="editable">
            <el-input v-model="info.image" readonly />
          </el-form-item>
        </el-descriptions-item>
        <el-descriptions-item label="ClassName" span="2">
          <el-form-item class="editable">
            <el-input v-model="info.className" readonly />
          </el-form-item>
        </el-descriptions-item>
        <el-descriptions-item label="GoFuncPath" span="2">
          <el-form-item class="editable">
            <el-input v-model="info.golang.go" readonly />
          </el-form-item>
        </el-descriptions-item>
      </el-descriptions>
    </el-drawer>
  </el-form>
</template>

<script>
  import { defineComponent, reactive, ref, watchEffect } from 'vue'
  import { info1 } from '../../../mock/services/flow'

  export default defineComponent({
    name: 'Detail',
    props: {
      nodeName: {
        type: String,
        default: ''
      }
    },
    emits: ['hideDrawer'],
    setup(props) {
      const editable = ref(false)
      const visibilityBinding = ref(false)
      const nodeDetail = ref(null)
      // Simulate fetching backend data
      const info = reactive(info1)

      // Show node detail
      watchEffect(() => {
        const selectedNode = props.nodeName
        if (selectedNode) {
          console.log(selectedNode)
        }
      })

      function saveEdit() {
        editable.value = false
      }
      function resetField() {
        console.log(info1.replicas)
        console.log(info.replicas)
      }
      function cancelEdit() {
        resetField()
        editable.value = false
      }

      return {
        info,
        info1,
        editable,
        nodeDetail,
        saveEdit,
        cancelEdit,
        visibilityBinding
      }
    }
  })
</script>

<style scoped>
  .description ::v-deep(.el-form-item) {
    margin-bottom: 0;
  }
  .editable ::v-deep(.el-input__inner) {
    border-color: #fff;
  }
</style>
