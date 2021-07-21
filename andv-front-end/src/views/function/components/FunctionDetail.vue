<template>
  <a-drawer :visible="visible"
            :width="810"
            @close="onClose">
    <a-spin tip="Loading..."
            :spinning="loadingDetail">
      <!-- function Info -->
      <a-form layout="inline"
              :form="form"
              hide-required-mark>
        <a-descriptions bordered
                        size="small">
          <a-row slot="title"
                 type="flex"
                 justify="space-between"
                 align="middle"
                 :style="{paddingRight:'32px'}">
            <span>Function Info</span>
            <a-button type="primary"
                      v-if="!editable"
                      @click="onChgEditable"> Edit
            </a-button>
            <span v-else>
              <a-button type="primary"
                        @click="saveEdit"
                        :style="{marginRight:'16px'}"
                        :loading="loadingSave">
                Save
              </a-button>
              <a-button type="dashed"
                        @click="cancelEdit">
                Cancel
              </a-button>

            </span>
          </a-row>

          <a-descriptions-item label="Name">
            <a-form-item>
              <a-input v-decorator="['name',
                                     {
                                       rules: [{ required: true, message: 'Please input your Function name!' }],
                                     }
                       ]"
                       class="inputDefault"
                       :disabled="true"
                       :class="{editable:true}" />
            </a-form-item>
          </a-descriptions-item>
          <a-descriptions-item label="Runtime"
                               :span="2">
            {{ currentFuncionInfo.runtime }}
          </a-descriptions-item>
          <a-descriptions-item label="Classname"
                               :span="3">
            <a-form-item :wrapper-col="{ span: 24 }"
                         :style="{width:'100%'}">
              <a-input v-decorator="['className',
                                     {
                                       rules: [{ required: true, message: 'Please input your className!' }],
                                     }
                       ]"
                       class="inputDefault"
                       :disabled="!editable"
                       :class="{editable:!editable}" />
            </a-form-item>
          </a-descriptions-item>
          <a-descriptions-item :span="3">
            <span slot="label">
              <span :style="{marginRight:'8px'}">Input</span>
              <a-button icon="plus"
                        shape="circle"
                        type="dashed"
                        size="small"
                        v-show="false"
                        @click="addInput" />
            </span>
            <a-form-item :wrapper-col="{ span: 24 }"
                         :style="{width:'100%'}"
                         v-for="item in inputs"
                         :key="item.key">
              <a-row type="flex"
                     :gutter="8">
                <a-col flex="1">
                  <a-input v-decorator="[item.key,
                                         {
                                           rules: [{ required: true, message: 'Please input your Input!' }],
                                         }
                           ]"
                           class="inputDefault"
                           :disabled="true"
                           :class="{editable:true}" />
                </a-col>
                <a-col>
                  <a-button icon="minus"
                            shape="circle"
                            type="dashed"
                            size="small"
                            v-show="editable && inputs.length!==1"
                            @click="rmInput(item.key)" />
                </a-col>
              </a-row>
            </a-form-item>

          </a-descriptions-item>
          <a-descriptions-item label="Output"
                               :span="3">
            <a-form-item :wrapper-col="{ span: 24 }"
                         :style="{width:'100%'}">
              <a-input v-decorator="['output',
                                     {
                                       rules: [{ required: true, message: 'Please input your Output!' }],
                                     }
                       ]"
                       class="inputDefault"
                       :disabled="!editable"
                       :class="{editable:!editable}" />
            </a-form-item>
          </a-descriptions-item>
          <a-descriptions-item label="File"
                               :span="3"
                               v-if="editable">
            <a-form-item :wrapper-col="{ span: 24 }"
                         :style="{width:'100%'}">
              <div class="dropbox">
                <a-upload-dragger v-decorator="[
                                    'data',
                                    {
                                      valuePropName: 'fileList',
                                      getValueFromEvent: normFile,
                                    },
                                  ]"
                                  :before-upload="fbeforeUpload"
                                  name="data">
                  <p class="ant-upload-drag-icon">
                    <a-icon type="inbox" />
                  </p>
                  <p class="ant-upload-text">
                    Click or drag file to this area to upload
                  </p>
                  <p class="ant-upload-hint">
                    Only jar files are supported
                  </p>
                </a-upload-dragger>
              </div>
            </a-form-item>
          </a-descriptions-item>
        </a-descriptions>
      </a-form>
      <a-descriptions title="Stats"
                      bordered
                      size="small"
                      :column="2"
                      :style="{margin:'24px 0'}">
        <a-descriptions-item label="Received">
          {{ currentFuncionInfo.receivedTotal || 0 }}
        </a-descriptions-item>
        <a-descriptions-item label="Processed Successfully">
          {{ currentFuncionInfo.processedSuccessfullyTotal || 0 }}
        </a-descriptions-item>
        <a-descriptions-item label="System Exceptions">
          {{ currentFuncionInfo.systemExceptionsTotal || 0 }}
        </a-descriptions-item>
        <a-descriptions-item label="Avg Process Latency">
          {{ currentFuncionInfo.avgProcessLatency || 0 }}
        </a-descriptions-item>
      </a-descriptions>
      <a-descriptions title="Status"
                      bordered
                      size="small"
                      :column="2"
                      :style="{margin:'24px 0'}">
        <a-descriptions-item label="Number of instances">
          {{ currentFuncionInfo.statusInfo && currentFuncionInfo.statusInfo.numInstances || 0 }}
        </a-descriptions-item>
        <a-descriptions-item label="Number of running">
          {{ currentFuncionInfo.statusInfo && currentFuncionInfo.statusInfo.numRunning || 0 }}
        </a-descriptions-item>
      </a-descriptions>
    </a-spin>
  </a-drawer>
</template>
<script>
import { uid } from 'uid'
import { update } from '@/api/func'

export default {
  data () {
    return {
      editable: false,
      form: this.$form.createForm(this),
      loadingSave: false,
      inputs: [],
      file: ''
    }
  },
  props: {
    visible: {
      type: Boolean,
      default: false
    },
    currentFuncionInfo: {
      type: Object,
      default: () => { }
    },
    loadingDetail: {
      type: Boolean,
      default: false
    }
  },
  methods: {
    onClose () {
      this.editable = false
      this.loadingSave = false
      this.inputs = []

      this.$parent.currentFuncionInfo = {}
      this.$parent.closeDetail()
    },
    onReset () {
      const inputs = {}
      const inputArr = this.currentFuncionInfo?.input?.map((input, i) => {
        const key = `input_${i}`
        Object.assign(inputs, { [key]: input })
        return { key, input }
      })

      this.inputs = inputArr

      this.form.setFieldsValue({
        name: this.currentFuncionInfo?.name,
        className: this.currentFuncionInfo?.className,
        output: this.currentFuncionInfo?.output,
        ...inputs
      })
    },
    onChgEditable () {
      this.editable = true
      this.onReset()
    },
    cancelEdit () {
      this.onReset()
      this.editable = false
      setTimeout(() => { // 恢复删除的
        this.onReset()
      })
    },
    addInput () {
      const inputName = `input_${uid(3)}`
      this.inputs = [...this.inputs, { key: inputName, input: '' }]
    },
    rmInput (key) {
      this.inputs = this.inputs?.filter(input => input.key !== key)
    },
    saveEdit () {
      this.form.validateFields((err, values) => {
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
            async onOk () {
              try {
                await update(functionName, data)
                  .then((res) => {
                    _this.$parent.refresh()
                    _this.$parent.closeDetail()
                    _this.$notification.success({ message: `function "${functionName}" created successfully` })
                  })
              } catch (error) {
                const errMessage = error.response.data.reason
                _this.$notification.error({ message: ` funciton "${functionName}" creation failed, because ${errMessage}` })
              }
            }
          })
          this.file = ''
        })
    },
    normFile (e) {
      console.log('Upload event:', e)
      if (Array.isArray(e)) {
        return [e[e.length - 1]]
      }
      if (e && e.fileList.length > 0) return [e.fileList[e.fileList.length - 1]]
      return []
    },
    fbeforeUpload (file) {
    this.file = file
      return false
    }
  },
  computed: {
    listenFuncChange () {
      const { visible, currentFuncionInfo } = this
      return { visible, currentFuncionInfo }
    }
  },
  watch: {
    listenFuncChange () {
      if (this.visible) {
        this.onReset()
      }
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
  border-color: #00000000;
}
.editable:hover {
  border-color: #00000000;
}
</style>
