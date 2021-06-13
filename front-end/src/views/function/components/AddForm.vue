<template>
  <a-drawer :width="720" :visible="visible" :body-style="{ paddingBottom: '40px' }" @close="onClose">
    <a-form :form="form" layout="vertical">
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
                                rules: [{ required: true, message: 'Please input your Input!' }],
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
        <!-- <a-breadcrumb>
          <a-breadcrumb-item v-for="(value,index) in input"><a @click="editInput(index)">{{value}}</a>
            <a-icon type="close-circle" @click='deleteInputs(index)' />
          </a-breadcrumb-item>
        </a-breadcrumb> -->
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
                rules: [{ required: true, message: 'Please enter log-topic' }],
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
  import { addFunc } from '@/api/func'
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
        // console.log('reset')
        this.form.resetFields()
      },
      onSub () {
        this.form.validateFields((err, values) => {
          if (err) return
          console.log(values)
          // console.log('Received values of form: ', values)
          // values.inputs = this.input
          // console.log(values)
          // console.log(values)
          addFunc(values.functionName, values)
            .then((res) => {
              // console.log("success!")
            })
        })
      },
      onRuntimeChg (value) {
        this.form.setFieldsValue({
          runtime: value
        })
      },
      normFile (e) {
        // console.log('Upload event:', e);
        if (Array.isArray(e)) {
          return [e[e.length - 1]]
        }
        if (e && e.fileList.length > 0) return [e.fileList[e.fileList.length - 1]]
        return []
      },
      fbeforeUpload (file) {
        this.fileList = [...this.fileList, file]
        console.log(this.fileList)
        return false
      },
      addInput () {
        this.input.push('')
        // console.log(this.input)
      },
      rmInput (index) {
        this.input.splice(index, 1)
        // console.log(key)
      }
      // enterInput(e) { //输入input(支持逐个输入)
      //   const value = e.target.value;
      //   var pattern = /\ /;
      //   // console.log(pattern.test(value));
      //   if (!value) {
      //     this.$message.error("不能为空!");
      //     return
      //   } else if (pattern.test(value)) {
      //     this.$message.error("不能含有空格！！");
      //     return
      //   }
      //   for (var i = 0; i < this.input.length; i++) {
      //     if (value == this.input[i]) {
      //       this.$message.error("已有该input！");
      //       return
      //     }
      //   }
      //   if (this.isEdit != -1) {
      //     this.input.splice(this.isEdit, 1, value);
      //     // this.form.resetFields('inputs');
      //   } else {
      //     this.input.push(value)
      //     // this.form.resetFields('inputs');
      //   }
      //   this.isEdit = -1
      // },
      // deleteInputs(index) {
      //   this.input.splice(index, 1);
      // },
      // editInput(index) {
      //   this.form.setFieldsValue({
      //     inputs: this.input[index]
      //   })
      //   this.isEdit = index
      //   // console.log(this.form)
      // }
    }
  }
</script>
