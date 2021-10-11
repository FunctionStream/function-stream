<template>
  <el-drawer size="40%" @open="onOpen" @close="onClose">
    <!-- function info -->
    <el-form
      ref="infoRef"
      v-loading="loadingDetail"
      :rules="rules"
      style="padding: 0 20px"
      :model="info"
      label-position="top"
    >
      <el-descriptions class="inputMargin" title="function info" border size="small">
        <template #extra>
          <el-button v-if="!editable" type="primary" size="small" @click="editable = true"> Edit </el-button>
          <span v-else>
            <el-button type="primary" :style="{ marginRight: '16px' }" size="small" @click="saveEdit()">
              Save
            </el-button>
            <el-button size="small" @click="cancelEdit('info')"> Cancel </el-button>
          </span>
        </template>
        <el-descriptions-item label="Name">
          <el-form-item :class="{ editable: !editable }">
            <el-input v-model="info.name" readonly />
          </el-form-item>
        </el-descriptions-item>
        <el-descriptions-item label="Runtime" :span="2">
          {{ currentFunctionInfo.runtime }}
        </el-descriptions-item>
        <el-descriptions-item label="Classname" :span="3">
          <el-form-item
            :class="{ editable: !editable }"
            prop="className"
            :wrapper-col="{ span: 24 }"
            :style="{ width: '100%' }"
          >
            <el-input v-model="info.className" :readonly="!editable" :class="{ editable: !editable }" />
          </el-form-item>
        </el-descriptions-item>
        <el-descriptions-item label="input" :span="3">
          <el-form-item
            v-for="item in inputs.value"
            :key="item.key"
            prop="input"
            :wrapper-col="{ span: 24 }"
            :style="{ width: '100%' }"
          >
            <el-input v-model="item.input" disabled readonly :class="{ editable: !editable }" />
          </el-form-item>
        </el-descriptions-item>
        <el-descriptions-item label="Output" :span="3">
          <el-form-item prop="output">
            <el-input v-model="info.output" :readonly="!editable" :class="{ editable: !editable }" />
          </el-form-item>
        </el-descriptions-item>
        <el-descriptions-item v-if="editable" label="File" :span="3">
          <el-upload
            drag
            name="data"
            :on-change="getFile"
            action=""
            :auto-upload="false"
            class="upload"
            accept=".jar"
            style="
               {
                width: 100%;
              }
            "
          >
            <i class="el-icon-upload"></i>
            <p class="el-upload-text">Click or drag file to this area to upload</p>
            <p class="el-upload-hint">Only jar files are supported</p>
          </el-upload>
        </el-descriptions-item>
      </el-descriptions>
      <el-descriptions title="Stats" border size="small" :column="2" :style="{ margin: '24px 0' }">
        <el-descriptions-item label="Received">
          {{ currentFunctionInfo.receivedTotal || 0 }}
        </el-descriptions-item>
        <el-descriptions-item label="Processed Successfully">
          {{ currentFunctionInfo.processedSuccessfullyTotal || 0 }}
        </el-descriptions-item>
        <el-descriptions-item label="System Exceptions">
          {{ currentFunctionInfo.systemExceptionsTotal || 0 }}
        </el-descriptions-item>
        <el-descriptions-item label="Avg Process Latency">
          {{ currentFunctionInfo.avgProcessLatency || 0 }}
        </el-descriptions-item>
      </el-descriptions>
      <el-descriptions title="Status" border size="small" :column="2" :style="{ margin: '24px 0' }">
        <el-descriptions-item label="Number of instances">
          {{ (currentFunctionInfo.statusInfo && currentFunctionInfo.statusInfo.numInstances) || 0 }}
        </el-descriptions-item>
        <el-descriptions-item label="Number of running">
          {{ (currentFunctionInfo.statusInfo && currentFunctionInfo.statusInfo.numRunning) || 0 }}
        </el-descriptions-item>
      </el-descriptions>
    </el-form>
  </el-drawer>
</template>

<script>
  import { update } from '@/api/func'
  import { reactive, ref } from '@vue/runtime-core'
  import { ElMessage, ElMessageBox } from 'element-plus'
  import { uid } from 'uid'
  export default {
    name: 'FunctionDetailVue',
    props: {
      visible: {
        type: Boolean,
        default: false
      },
      loadingDetail: {
        type: Boolean,
        default: false
      },
      currentFunctionInfo: {
        type: Object,
        default: () => {}
      }
    },
    setup(props) {
      const infoRef = ref(null)
      // open and close drawer
      const info = reactive({})
      const editable = ref(false)
      const loadingSave = ref(false)
      const inputs = reactive({})
      const onOpen = () => {
        info.value = props.currentFunctionInfo
      }
      const onClose = () => {
        editable.value = false
        loadingSave.value = false
      }
      // get uploaded file
      const file = reactive({})
      const getFile = (f) => {
        file.value = f.raw
      }
      // operation about input field
      const addInput = () => {
        const inputName = `input_${uid(3)}`
        inputs.value = [...inputs.value, { key: inputName, input: '' }]
      }
      const rmInput = (key) => {
        inputs.value = inputs.value.filter((input) => input.key !== key)
      }
      // reset the function detail form
      const onReset = () => {
        const inputArr = props.currentFunctionInfo?.input?.map((input, i) => {
          const key = `input_${i}`
          inputs.value = {
            [key]: input
          }
          return { key, input }
        })
        inputs.value = inputArr
        Object.keys(props.currentFunctionInfo).forEach((item) => {
          info[item] = props.currentFunctionInfo[item]
        })
        infoRef.value.clearValidate()
      }
      const cancelEdit = () => {
        editable.value = false
        onReset()
      }
      // save edit function detail
      const rules = {
        Name: [{ require: true, message: 'Please input your Function name!', trigger: 'change' }],
        className: [{ required: true, message: 'Please input your className!', trigger: 'change' }],
        input: [{ required: true, message: 'Please input your Input!', trigger: 'change' }],
        output: [{ required: true, message: 'Please input your Output!', trigger: 'change' }]
      }
      const saveEdit = () => {
        infoRef.value.validate((valid) => {
          if (valid) {
            const functionName = info.value.name
            const data = new FormData()
            if (file.value) {
              const fileName = file.value.name
              if (fileName.substr(fileName.lastIndexOf('.') + 1) !== 'jar') {
                ElMessage.error('Only jar files are supported, please check the type of your uploaded file.')
                return
              } else {
                data.append('data', file.value)
              }
            }
            const functionConfig = info
            delete functionConfig.data
            delete functionConfig.Name
            data.append('functionConfig', new Blob([JSON.stringify(functionConfig)], { type: 'application/json' }))
            ElMessageBox.confirm('Are you sure to edit this function?', 'Tip', {
              confirmButtonText: 'OK',
              cancelButtonText: 'Cancel',
              type: 'warning'
            }).then(() => {
              update(functionName, data)
                .then(() => {
                  editable.value = false
                  ElMessage({
                    type: 'success',
                    message: 'Edit successfully'
                  })
                })
                .catch((err) => {
                  if (err.response) {
                    const errMessage = err.response.data.reason
                    ElMessage({
                      type: 'error',
                      message: ` funciton "${functionName}" creation failed, because ${errMessage}`
                    })
                  }
                  onReset()
                })
              editable.value = false
            })
            file.value = ''
          } else {
            console.log('error commit')
            return false
          }
        })
      }
      return {
        onReset,
        onClose,
        onOpen,
        getFile,
        cancelEdit,
        saveEdit,
        addInput,
        rmInput,
        info,
        editable,
        inputs,
        infoRef,
        rules
      }
    },
    computed: {
      listenFuncChange() {
        const { visible, currentFunctionInfo } = this
        return { visible, currentFunctionInfo }
      }
    },
    watch: {
      listenFuncChange() {
        if (this.visible) {
          this.onReset()
        }
        if (!this.loadingDetail) {
          Object.assign(this.info, this.currentFunctionInfo)
          this.onReset()
        }
      }
    }
  }
</script>
<style scoped>
  .editable ::v-deep(.el-input__inner) {
    border-color: #fff;
  }
  .upload ::v-deep(.el-upload) {
    width: 100%;
  }
  .upload ::v-deep(.el-upload-dragger) {
    width: 100%;
  }
  .inputMargin ::v-deep(.el-form-item) {
    margin-bottom: 0;
  }
</style>
