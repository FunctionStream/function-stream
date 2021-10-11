<template>
  <el-drawer :size="460" @close="onClose">
    <el-form ref="TriggerFormRef" v-loading="visibleTrigger" :model="TriggerForm" :rules="rules">
      <el-row>
        <el-form-item v-if="currentFunction.name" prop="functionName">
          <span>functionName</span>
          <el-select
            v-model="TriggerForm.functionName"
            placeholder="please select a function"
            style="width: 368px"
            size="small"
          >
            <el-option v-for="item in data" :key="item.index" :label="item.name" :value="item.key">
              {{ item.name }}
            </el-option>
          </el-select>
        </el-form-item>
      </el-row>
      <el-row>
        <el-form-item prop="data">
          <span>data</span>
          <el-input
            v-model="TriggerForm.data"
            autosize
            type="textarea"
            placeholder="please enter the data"
            style="width: 368px"
          />
        </el-form-item>
      </el-row>
    </el-form>
    <el-row type="flex" justify="end">
      <el-button type="primary" :loading="triggering" style="margin-bottom: 24px" @click="onSub('TriggerForm')">
        Trigger
      </el-button>
    </el-row>
    <el-card class="box-card">
      <div class="clearfix">
        <span>Result</span>
        <span style="float: right; padding: 3px 0">{{ triggerResultType }}</span>
      </div>
      <div :style="{ minHeight: '64px' }" class="text item">
        <span style="word-break: break-all">{{ triggerResult }}</span>
      </div>
    </el-card>
  </el-drawer>
</template>

<script>
  import { triggerFunc } from '@/api/func'
  import { reactive, ref, watch } from '@vue/runtime-core'
  import { ElMessage, ElMessageBox } from 'element-plus'
  import { uid } from 'uid'

  export default {
    name: 'TriggerVue',
    props: {
      visible: {
        type: Boolean,
        default: false
      },
      visibleTrigger: {
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
    setup(props) {
      const TriggerForm = reactive({})
      const TriggerFormRef = ref(null)
      const triggerResult = ref('')
      const triggerResultType = ref('')
      const triggering = ref(false)
      const rules = reactive({
        functionName: [{ required: true, message: 'Please select a function!', trigger: 'blur' }],
        data: [{ required: true, message: 'Please select a function!', trigger: 'blur' }]
      })

      const onClose = () => {
        triggering.value = false
      }
      const onSub = (subName) => {
        triggerResult.value = ''
        triggerResultType.value = ''
        TriggerFormRef.value.validate((valid) => {
          if (valid) {
            triggering.value = true
            const values = TriggerForm
            const formData = new FormData()
            const functionName = values.functionName
            const functionData = values.data
            //参数处理
            if (typeof functionData === 'string') {
              formData.append('data', JSON.stringify(functionData))
            } else {
              formData.append('data', functionData)
            }
            triggerFunc(functionName, formData)
              .then((res) => {
                triggerResult.value = res
                triggerResultType.value = typeof res
              })
              .catch((err) => {
                if (err.response) {
                  const errMessage = err.response.data.reason
                  ElMessage({
                    type: 'error',
                    message: ` Funciton "${functionName}" trigger failed, because ${errMessage}`
                  })
                }
              })
              .finally(() => {
                setTimeout(() => {
                  triggering.value = false
                }, 500)
              })
          }
        })
      }
      const currentFunc = () => {
        TriggerFormRef.value.setFieldsValue({
          functionName: props.currentFunction.name
        })
      }
      return {
        TriggerForm,
        TriggerFormRef,
        triggerResult,
        triggerResultType,
        triggering,
        rules,
        onClose,
        onSub,
        currentFunc
      }
    }
  }
</script>

<style>
  .text {
    font-size: 14px;
  }
  .item {
    padding: 18px 0;
  }
  .clearfix {
    border-bottom: 1px solid;
  }
  .clearfix:after {
    display: table;
    content: '';
  }
  .clearfix:after {
    clear: both;
  }
  .box-card {
    width: 368px;
    display: block;
    margin-left: 46px;
  }
  .el-form-item__content {
    margin-left: 46px !important;
  }
  .el-row {
    width: 416px;
  }
</style>
