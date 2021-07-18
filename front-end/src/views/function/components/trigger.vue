<template>
  <el-drawer :visible="visible" :size="460" @click="onClose">
    <el-form ref="TriggerForm" :model="TriggerForm" :rules="rules">
      <el-row>
        <el-form-item v-if="currentFunction.name" prop="functionName">
          <span>functionName</span>
          <el-select v-model="TriggerForm.functionName" placeholder="please select a function" style="width: 368px;" size="small">
            <el-option
              v-for="(item,index) in data"
              :key="item.index"
              :label="item.name"
              :value="item.key">
              {{item.name}}
            </el-option>
          </el-select>
        </el-form-item>
      </el-row>
      <el-row>
        <el-form-item prop="data">
          <span>data</span>
          <el-input
            autosize
            type="textarea"
            placeholder="please enter the data"
            v-model="TriggerForm.data"
            style="width: 368px;"/>
        </el-form-item>
      </el-row>
    </el-form>
    <el-row type="flex" justify="end">
      <el-button type="primary" :loading="triggering" @click="onSub('TriggerForm')" style="margin-bottom: 24px"> Trigger </el-button>
    </el-row>
    <el-card class="box-card">
      <div slot="header" class="clearfix">
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

  export default {
    name: 'TriggerVue',
    data () {
      return {
        TriggerForm: {},
        triggerResult: '',
        triggerResultType: '',
        triggering: false,
        rules: {
          functionName: [
            { required: true, message: 'Please select a function!', trigger: 'blur' }
          ],
          data: [
            { required: true, message: 'Please select a function!', trigger: 'blur' }
          ]
        }
      }
    },
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
    methods: {
      onClose () {
        this.$parent.closeTrigger()
      },
      onSub(subName) {
        this.triggerResult = ''
        this.triggerResultType = ''
        this.$refs[subName].validate((err) => {
          if (err) {
            this.triggering = true
            const values = this.TriggerForm
            const formData = new FormData()
            const functionName = values.functionName
            const functionData = values.data
            //参数处理
            if (typeof functionData === 'string') {
              formData.append('data', JSON.stringify(functionData))
            } else {
              formData.append('data', functionData)
            }
            const _this = this

            async function onOk() {
              try {
                await triggerFunc(functionName, formData)
                  .then((res) => {
                    _this.triggerResult = res
                    _this.triggerResultType = typeof res
                  })
              } catch (error) {
                _this.$message.error('Function trigger failed!')
              } finally {
                setTimeout(() => {
                  _this.triggering = false
                }, 500)
              }
            }
            onOk()
          }
        })
      }
    },
    watch: {
      currentFunc () {
        this.TriggerForm.setFieldsValue({
          functionName : this.currentFunction.name
        })
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
  .clearfix{
    border-bottom: 1px solid;
  }
  .clearfix:after {
    display: table;
    content: "";
  }
  .clearfix:after {
    clear: both
  }
  .box-card{
    width: 368px;
    display: block;
    margin-left: 46px;
  }
  .el-form-item__content {
    margin-left: 46px !important;
  }
  .el-row{
    width: 416px;
  }
</style>
