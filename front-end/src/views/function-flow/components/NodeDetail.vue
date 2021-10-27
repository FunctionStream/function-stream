<template>
  <el-form :model="info">
    <el-drawer v-model="visibilityBinding" size="40%" :direction="direction">
      <el-descriptions title="Node detail" :column="2" border class="description" style="padding: 0 20px">
        <template #extra>
          <el-button v-show="!editable" type="primary" @click="editable = true">Edit</el-button>
          <span v-show="editable">
            <el-button type="primary" :style="{ marginRight: '16px' }" @click="saveEdit()"> Save </el-button>
            <el-button @click="cancelEdit()"> Cancel </el-button>
          </span>
        </template>
        <el-descriptions-item label="Replicas">
          <el-form-item :class="{ editable: !editable }">
            <el-input v-model="info.replicas" />
          </el-form-item>
        </el-descriptions-item>
        <el-descriptions-item label="MaxReplicas">
          <el-form-item :class="{ editable: !editable }">
            <el-input v-model="info.maxReplicas" />
          </el-form-item>
        </el-descriptions-item>
        <el-descriptions-item label="CPU">
          <el-form-item :class="{ editable: !editable }">
            <el-input v-model="info.resources.requests.cpu" />
          </el-form-item>
        </el-descriptions-item>
        <el-descriptions-item label="Memory">
          <el-form-item :class="{ editable: !editable }">
            <el-input v-model="info.resources.requests.memory" />
          </el-form-item>
        </el-descriptions-item>
        <el-descriptions-item label="ClassName" span="2">
          <el-form-item class="editable">
            <el-input v-model="info.className" disabled />
          </el-form-item>
        </el-descriptions-item>
        <el-descriptions-item label="GoFuncPath" span="2">
          <el-form-item class="editable">
            <el-input v-model="info.golang.go" disabled />
          </el-form-item>
        </el-descriptions-item>
        <el-descriptions-item label="Image">
          <el-form-item class="editable">
            <el-input v-model="info.replicas" disabled />
          </el-form-item>
        </el-descriptions-item>
      </el-descriptions>
    </el-drawer>
  </el-form>
</template>

<script>
  import { defineComponent, ref } from 'vue'

  export default defineComponent({
    name: 'Detail',
    emits: ['hideDrawer'],
    setup(props, { emit }) {
      const direction = ref('rtl')
      const editable = ref(false)
      const visibilityBinding = ref(false)

      const info = {
        image: 'streamnative/pulsar-functions-go-sample:2.8.1',
        name: 'test-ex',
        autoAck: true,
        className: 'exclamation_function.ExclamationFunction',
        forwardSourceMessageProperty: true,
        MaxPendingAsyncRequests: 1000,
        replicas: 1,
        maxReplicas: 5,
        logTopic: 'persistent://public/default/logging-function-logs',
        input: {
          topics: ['persistent://public/default/a1'],
          typeClassName: 'java.lang.String'
        },
        output: {
          topic: 'persistent://public/default/a2',
          typeClassName: 'java.lang.String'
        },
        pulsar: {
          pulsarConfig: 'mesh-test-pulsar'
        },
        resources: {
          requests: {
            cpu: '0.1',
            memory: '10M'
          },
          limits: {
            cpu: '0.2',
            memory: '200M'
          }
        },
        golang: {
          go: '/pulsar/examples/go-exclamation-func'
        }
      }

      function saveEdit() {
        editable.value = false
      }
      function cancelEdit() {
        editable.value = false
      }

      return {
        direction,
        info,
        editable,
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
