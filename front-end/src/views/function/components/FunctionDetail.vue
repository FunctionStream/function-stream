<template>
  <el-drawer size="40%" @open="onOpen" @close="onClose">
    <!-- function info -->
    <el-form
      ref="currentFunctionInfo"
      v-loading="loadingDetail"
      :rules="rules"
      style="padding: 0 20px"
      :model="currentFunctionInfo"
    >
      <el-descriptions title="function info" border size="small">
        <template #extra>
          <el-button v-if="!editable" type="primary" size="small" @click="onChgEditable"> Edit </el-button>
          <span v-else>
            <el-button
              type="primary"
              :style="{ marginRight: '16px' }"
              size="small"
              @click="saveEdit('currentFunctionInfo')"
            >
              Save
            </el-button>
            <el-button size="small" @click="cancelEdit"> Cancel </el-button>
          </span>
        </template>
        <el-descriptions-item label="Name" class="inputDefault">
          <el-form-item v-if="editable">
            <el-input v-model="currentFunctionInfo.name" readonly />
          </el-form-item>
          <el-input v-else v-model="currentFunctionInfo.name" readonly />
        </el-descriptions-item>
        <el-descriptions-item label="Runtime" :span="2">
          {{ currentFunctionInfo.runtime }}
        </el-descriptions-item>
        <el-descriptions-item class="inputDefault" label="Classname" :span="3">
          <el-form-item v-if="editable" prop="className" :wrapper-col="{ span: 24 }" :style="{ width: '100%' }">
            <el-input
              v-model="currentFunctionInfo.className"
              class="inputDefault"
              :readonly="!editable"
              :class="{ editable: !editable }"
            />
          </el-form-item>
          <el-input v-else v-model="currentFunctionInfo.className" readonly />
        </el-descriptions-item>
        <el-descriptions-item label="input" :span="3">
          <el-form-item v-if="editable" prop="input">
            <el-input
              v-model="currentFunctionInfo.input"
              class="inputDefault"
              :readonly="!editable"
              :class="{ editable: !editable }"
            />
          </el-form-item>
          <el-input v-else v-model="currentFunctionInfo.input" readonly />
        </el-descriptions-item>
        <el-descriptions-item class="inputDefault" label="Output" :span="3">
          <el-form-item v-if="editable" prop="output">
            <el-input
              v-model="currentFunctionInfo.output"
              class="inputDefault"
              :readonly="!editable"
              :class="{ editable: !editable }"
            />
          </el-form-item>
          <el-input v-else v-model="currentFunctionInfo.output" readonly />
        </el-descriptions-item>
        <el-descriptions-item v-if="editable" class="uploadBox" label="File" :span="3">
          <el-upload
            drag
            name="data"
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
        <el-descriptions-item label="Number of instances"> 0 </el-descriptions-item>
        <el-descriptions-item label="Number of running"> 0 </el-descriptions-item>
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
          funcName: [{ require: true, message: 'Please input your Function name!', trigger: 'change' }],
          className: [{ required: true, message: 'Please input your className!', trigger: 'change' }],
          input: [{ required: true, message: 'Please input your Input!', trigger: 'change' }],
          output: [{ required: true, message: 'Please input your Output!', trigger: 'change' }]
        },
        inputs: [],
        file: '',
        beforeEditInfo: {}
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
      }
    },
    methods: {
      onReset() {
        const inputs = {}
        const inputArr = this.currentFunctionInfo?.input?.map((input, i) => {
          const key = `input_${i}`
          Object.assign(inputs, { [key]: input })
          return { key, input }
        })

        this.inputs = inputArr

        /* this.form.setFieldsValue({
          name: this.currentFunctionInfo?.name,
          className: this.currentFunctionInfo?.className,
          output: this.currentFunctionInfo?.output,
          ...inputs
        }) */
      },
      onOpen() {
        console.log('loadingdetail in onopen', this.loadingDetail)
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
        setTimeout(() => {
          this.onReset()
        })
      },
      saveEdit(form) {
        this.$refs[form].validate((valid) => {
          if (valid) {
            alert('submit')
          } else {
            console.log('error commit')
            return false
          }
        })
        this.$refs[form].validateField((err, values) => {
          if (err) return
          console.log(values)
          const functionName = values.name
          const data = new FormData()
          if (values.data) {
            data.append('data', this.file)
          }
          const functionConfig = values
          delete functionConfig.data
          delete functionConfig.Name // 参数处理
          data.append('functionConfig', new Blob([JSON.stringify(functionConfig)], { type: 'application/json' }))
          const _this = this
          this.$confirm({
            title: 'Are you sure to create this function?',
            content: 'Some descriptions',
            okType: 'primary',
            async onOk() {
              try {
                await update(functionName, data).then((res) => {
                  _this.$parent.refresh()
                  _this.$parent.closeDetail()
                  _this.$notification.success({ message: `function "${functionName}" created successfully` })
                })
              } catch (error) {
                const errMessage = error.response.data.reason
                _this.$notification.error({
                  message: ` funciton "${functionName}" creation failed, because ${errMessage}`
                })
              }
            }
          })
          this.file = ''
        })
      }
    }
  }
</script>

<style scoped>
  .inputDefault {
    background: #00000000;
    cursor: auto;
    color: #000000a6;
  }
  .editable {
    border-color: #fff;
  }
  .editable:hover {
    border-color: #00000000;
  }
</style>
