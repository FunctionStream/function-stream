<template>
  <el-drawer :visible="visible" size="40%" @open="onOpen" @close="onClose">
    <!-- function info -->
    <el-form ref="info" v-loading="loadingDetail" :rules="rules" style="padding: 0 20px" :model="info">
      <el-descriptions class="inputMargin" title="function info" border size="small">
        <template #extra>
          <el-button v-if="!editable" type="primary" size="small" @click="onChgEditable"> Edit </el-button>
          <span v-else>
            <el-button type="primary" :style="{ marginRight: '16px' }" size="small" @click="saveEdit('info')">
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
            v-for="item in inputs"
            :key="item.key"
            prop="input"
            :wrapper-col="{ span: 24 }"
            :style="{ width: '100%' }"
          >
            <el-input v-model="item.input" :disabled="true" :class="{ editable: !editable }" />
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
            :auto-upload="false"
            class="upload"
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
    data() {
      return {
        editable: false,
        rules: {
          Name: [{ require: true, message: 'Please input your Function name!', trigger: 'change' }],
          className: [{ required: true, message: 'Please input your className!', trigger: 'change' }],
          input: [{ required: true, message: 'Please input your Input!', trigger: 'change' }],
          output: [{ required: true, message: 'Please input your Output!', trigger: 'change' }]
        },
        inputs: [],
        file: '',
        beforeEditInfo: {},
        info: {}
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
    },
    methods: {
      getFile(file) {
        this.file = file.raw
      },
      onReset() {
        const ref = 'info'
        const inputArr = this.currentFunctionInfo?.input?.map((input, i) => {
          const key = `input_${i}`
          Object.assign(this.inputs, { [key]: input })
          return { key, input }
        })

        console.log('reset', this.inputs)
        this.inputs = inputArr
        Object.assign(this.info, this.currentFunctionInfo)
        this.$refs[ref].clearValidate()
      },
      onOpen() {
        this.info = this.currentFunctionInfo
      },
      onClose() {
        this.editable = false
        this.loadingSave = false
        this.inputs = []

        this.$parent.currentFunctionInfo = {}
        this.$parent.$parent.closeDetail()
      },
      onChgEditable() {
        this.editable = true
        this.onReset()
      },
      cancelEdit() {
        this.onReset()
        this.editable = false
      },
      saveEdit(form) {
        this.$refs[form].validate((valid) => {
          if (valid) {
            const functionName = this.info.name
            const data = new FormData()
            if (this.file) {
              data.append('data', this.file)
            }
            const functionConfig = this.info
            delete functionConfig.data
            delete functionConfig.Name // 参数处理
            data.append('functionConfig', new Blob([JSON.stringify(functionConfig)], { type: 'application/json' }))
            const _this = this
            this.$confirm('Some descriptions', 'Are you sure to create this function?', {
              confirmButtonText: 'OK',
              cancelButtonText: 'Cancel',
              type: 'primary'
            }).then(() => {
              try {
                update(functionName, data).then((res) => {
                  console.log(this.$parent.$parent.refresh())
                  this.editable = false
                  // this.$parent.refresh()
                  // _this.$parent.closeDetail()
                  // _this.$notification.success({ message: `function "${functionName}" created successfully` })
                })
              } catch (error) {
                const errMessage = error.response.data.reason
                _this.$notification.error({
                  message: ` funciton "${functionName}" creation failed, because ${errMessage}`
                })
              }
            })
            this.file = ''
          } else {
            console.log('error commit')
            return false
          }
        })
      }
    }
  }
</script>

<style scoped>
<<<<<<< HEAD
  .editable :deep(.el-input__inner) {
    border-color: #fff;
  }
  .uploadBox :deep(.el-upload) {
    width: 100%;
  }
  .uploadBox :deep(.el-upload-dragger) {
    width: 100%;
=======
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
>>>>>>> 6eea3b8c3501c5bd1a3d4655366a85201edcdff5
  }
</style>
