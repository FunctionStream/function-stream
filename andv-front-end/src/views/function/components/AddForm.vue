<template>
  <a-drawer :width="720" :visible="visible" :body-style="{ paddingBottom: '40px' }" @close="onClose">
    <a-form :form="form" layout="vertical">
      <a-row>
        <a-form-item label="FunctionName">
          <a-input
            v-decorator="[
              'FunctionName',
              {
                rules: [{ required: true, message: 'Please enter the FunctionName' }],
              },
            ]"
            placeholder="Please enter the FunctionName"
            allowClear />
        </a-form-item>
      </a-row>
      <a-row>
        <span :style="{marginRight:'8px'}">Inputs</span>
        <a-form-item
          :wrapper-col="{ span: 24 }"
          :style="{width:'100%'}"
          style="margin-bottom: 0;"
          v-for="(item,index) in input"
          :key="index">
          <a-row
            type="flex"
            :gutter="8">
            <a-col flex="1">
              <a-input
                v-decorator="[`inputs[${index}]`,
                              {
                                rules: [{ required: true, message: 'Please add your input topics' }],
                              }
                ]"
                class="inputDefault"
                getFieldDecorator
                allowClear/>
            </a-col>
            <a-col>
              <a-button
                icon="minus"
                shape="circle"
                type="dashed"
                size="small"
                v-show="input.length!==1"
                @click="rmInput(index)"/>
            </a-col>
          </a-row>
        </a-form-item>
        <a-button type="dashed" style="width: 100%" @click="addInput()">
          <a-icon type="plus" /> Add inputs
        </a-button>
      </a-row>
      <a-row>
        <a-form-item label="Output">
          <a-input
            v-decorator="[
              'output',
              {
                rules: [{ required: true, message: 'Please enter the output topic' }],
                initialValue:'persistent://public/default/output-topic',
              },
            ]"
            placeholder="Please enter output"
            allowClear />
        </a-form-item>
      </a-row>
      <a-row>
        <a-form-item label="Log-topic">
          <a-input
            v-decorator="[
              'log-topic',
              {
                rules: [{ required: true, message: 'Please enter the log topic.' }],
                initialValue:'persistent://public/default/log-topic',
              },
            ]"
            placeholder="Please enter log-topic"
            allowClear />
        </a-form-item>
      </a-row>
      <a-row :gutter="16">
        <a-col :span="12">
          <a-form-item label="ClassName">
            <a-input
              v-decorator="[
                'className',
                {
                  rules: [{ required: true, message: 'Please enter className' }],
                },
              ]"
              placeholder="Please enter className"
              allowClear />
          </a-form-item>
        </a-col>
        <a-col :span="12">
          <a-form-item label="Runtime">
            <a-select
              v-decorator="[
                'runtime',
                {
                  rules: [{ required: true }],
                  initialValue:'JAVA'
                },
              ]"
              @change="onRuntimeChg">
              <a-select-option value="JAVA">
                JAVA
              </a-select-option>
              <a-select-option value="GO">
                GO
              </a-select-option>
            </a-select>
          </a-form-item>
        </a-col>
      </a-row>

      <a-form-item label="Dragger">
        <a-upload-dragger
          v-decorator="[
            'data',
            {
              rules: [{ required: true, message: 'Please dragger function file' }],
              valuePropName: 'fileList',
              getValueFromEvent: normFile,
            },
          ]"
          :before-upload="fbeforeUpload"
          name="data"
          accept=".jar">
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
      </a-form-item>
    </a-form>
    <div
      :style="{
        position: 'absolute',
        right: 0,
        bottom: 0,
        width: '100%',
        borderTop: '1px solid #e9e9e9',
        padding: '10px 16px',
        background: '#fff',
        textAlign: 'right',
        zIndex: 1,
      }">
      <a-button :style="{ marginRight: '16px' }" @click="onReset">
        reset
      </a-button>
      <a-button type="primary" @click="onSub">
        Add Function
      </a-button>
    </div>
  </a-drawer>
</template>

<script>
import { create } from '@/api/func'
  export default {
    data () {
      return {
        form: this.$form.createForm(this),
        input: [''],
        editable: false,
        isEdit: -1,
        fileList: ''
      }
    },
    props: {
      visible: {
        type: Boolean,
        default: false
      }
    },
    methods: {
      showDrawer () {
        this.$parent.showDrawer()
      },
      onClose () {
        this.$parent.closeDrawer()
      },
      onReset () {
        this.form.resetFields()
        this.input = ['']
      },
      onSub () {
        this.form.validateFields((err, values) => {
          if (err) return
          const functionName = values.FunctionName
          const data = new FormData()
          data.append('data', this.fileList)
          const functionConfig = values
          delete functionConfig.data
          delete functionConfig.FunctionName // 参数处理
          data.append('functionConfig', new Blob([JSON.stringify(functionConfig)], { type: 'application/json' }))
          const _this = this
          this.$confirm({
            title: 'Are you sure to create this function?',
            content: 'Some descriptions',
            okType: 'primary',
            async onOk () {
              try {
                await create(functionName, data)
                  .then((res) => {
                    _this.$parent.refresh()
                    _this.$parent.closeDrawer()
                    _this.$notification.success({ message: `function "${functionName}" created successfully` })
                  })
              } catch (error) {
                _this.$notification.error({ message: ` funciton "${functionName}" creation failed` })
              }
            }
          })
        })
      },
      onRuntimeChg (value) {
        this.form.setFieldsValue({
          runtime: value
        })
      },
      normFile (e) {
        if (Array.isArray(e)) {
          return [e[e.length - 1]]
        }
        if (e && e.fileList.length > 0) return [e.fileList[e.fileList.length - 1]]
        return []
      },
      fbeforeUpload (file) {
        this.fileList = file
        return false
      },
      addInput () {
        this.input.push('')
      },
      rmInput (index) {
        this.input.splice(index, 1)
      }
    }
  }
</script>
