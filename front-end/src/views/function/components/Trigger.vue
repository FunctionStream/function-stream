<template>
  <a-drawer :visible="visible" :width="460" @close="onClose">
    <a-form :form="form" layout="vertical" hide-required-mark>
      <a-row>
        <a-form-item label="Function Name" v-if="currentFunction.name">
          <a-select
            v-decorator="[
              'functionName',
              {
                rules: [{ required: true, message: 'Please select function!' }],
                initialValue: currentFunction.name,
              },
            ]"
          >
            <a-select-option v-for="item in data" :key="item.key" :value="item.name">
              {{ item.name }}
            </a-select-option>
          </a-select>
        </a-form-item>
      </a-row>
      <a-row>
        <a-form-item label="data">
          <a-textarea
            v-decorator="[
              'data',
              {
                initialValue: '',
              },
            ]"
            :auto-size="{ minRows: 1, maxRows: 5 }"
            allowClear
          />
        </a-form-item>
      </a-row>
    </a-form>
    <a-row type="flex" justify="end" :style="{ marginBottom: '24px' }">
      <a-button type="primary" :loading="triggering" @click="onSub"> Trigger </a-button>
    </a-row>
    <a-card title="Result" :extra="triggerResultType" size="small">
      <div :style="{ minHeight: '64px' }">
        {{ triggerResult }}
      </div>
    </a-card>
  </a-drawer>
</template>

<script>
import { triggerFunc } from '@/api/func'

export default {
  data () {
    return {
      form: this.$form.createForm(this),
      triggerResult: '',
      triggerResultType: '',
      triggering: false
    }
  },
  props: {
    visible: {
      type: Boolean,
      default: false
    },
    data: {
      type: Array,
      default: () => []
    },
    currentFunction: {
      type: Object,
      default: () => {}
    }
  },
  methods: {
    onClose () {
      this.$parent.closeTrigger()
    },
    onSub () {
      this.triggering = true
      this.triggerResult = ''
      this.triggerResultType = ''
      this.form.validateFields((err, values) => {
        if (err) return
        console.log('Received values of form: ', values)
        triggerFunc(values.functionName, values)
          .then((res) => {
            this.triggerResult = JSON.stringify(res)
            this.triggerResultType = typeof res
          })
          .finally(() => {
            setTimeout(() => {
              this.triggering = false
            }, 500)
          })
      })
    }
  },
  watch: {
    currentFunction () {
      this.form.setFieldsValue({
        functionName: this.currentFunction.name
      })
    }
  }
}
</script>
