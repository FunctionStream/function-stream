<template>
  <a-drawer :width="720" :visible="visible" :body-style="{ paddingBottom: '40px' }" @close="onClose">
    <a-form :form="form" layout="vertical" hide-required-mark>
      <a-row>
        <a-form-item label="Inputs" v-decorator="">
          <a-input
            v-decorator="[
              'inputs',
              {
                rules: [{ required: true, message: 'Please enter inputs' }],
                initialValue: 'persistent://public/default/input-topic',
              },
            ]"
            placeholder="Please enter inputs"
            allowClear />
        </a-form-item>
      </a-row>
      <a-row>
        <a-form-item label="Output">
          <a-input
            v-decorator="[
              'output',
              {
                rules: [{ required: true, message: 'Please enter output' }],
                initialValue: 'persistent://public/default/output-topic',
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
                initialValue: 'persistent://public/default/log-topic',
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
                  initialValue: 'JAVA',
                },
              ]"
              @change="onRuntimeChg">
              <a-select-option value="JAVA"> JAVA</a-select-option>
              <a-select-option value="GO"> GO</a-select-option>
            </a-select>
          </a-form-item>
        </a-col>
      </a-row>

      <a-form-item label="Dragger">
        <div class="dropbox">
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
            <p class="ant-upload-text">Click or drag file to this area to upload</p>
            <p class="ant-upload-hint">Only jar files are supported</p>
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
      <a-button :style="{ marginRight: '16px' }" @click="onReset"> reset</a-button>
      <a-button type="primary" @click="onSub"> Add Function</a-button>
    </div>
  </a-drawer>
</template>

<script>
export default {
  data () {
    return {
      form: this.$form.createForm(this)
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
    },
    onSub () {
      this.form.validateFields((err, values) => {
        if (!err) {
          console.log('Received values of form: ', values)
        }
      })
    },
    onRuntimeChg (value) {
      this.form.setFieldsValue({
        runtime: value
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
    fbeforeUpload () {
      return false
    }
  }
}
</script>
