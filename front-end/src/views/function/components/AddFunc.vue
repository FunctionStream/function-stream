<template>
  <el-drawer ref="addFuncDrawer" size="40%" title="AddFunction" @close="onClose">
    <el-form
      ref="addForm"
      class="addFunc"
      :rules="rules"
      :model="form"
      label-width="80px"
      label-position="top"
      style="width: 95%; margin: 0 auto"
      size="small"
      :inline="true"
    >
      <el-form-item label="FunctionName:" style="width: 100%" prop="functionName">
        <el-input v-model="form.functionName" placeholder="请输入FunctionName" clearable> </el-input>
      </el-form-item>
      <div class="topic">
        <a style="color: red; font-size: 14px"> *&nbsp; </a>
        <a style="font-size: 14px"> Inputs: </a>
      </div>
      <el-form-item
        v-for="(value, index) in form.inputs"
        :key="index"
        style="width: 100%; margin-top: 5px"
        :prop="'inputs.' + index"
        :rules="{
          required: true,
          message: '请输入Input',
          trigger: 'blur'
        }"
      >
        <el-input
          v-model="form.inputs[index]"
          :style="form.inputs.length === 1 ? 'width:100%' : 'width:95%'"
          placeholder="请输入Input"
          clearable
        >
        </el-input>
        <i v-show="form.inputs.length !== 1" class="el-icon-circle-close addInputIcon" @click="deleteInput(index)"> </i>
      </el-form-item>
      <el-form-item style="width: 100%">
        <el-button
          style="width: 100%; font-size: 1em; margin-top: 5px"
          size="mini"
          class="el-icon-circle-plus-outline"
          @click="addInput"
        >
        </el-button>
      </el-form-item>
      <el-form-item label="Output:" style="width: 100%" prop="output">
        <el-input v-model="form.output" placeholder="请输入Output" clearable> </el-input>
      </el-form-item>
      <el-form-item label="Log-topic:" style="width: 100%" prop="logTopic">
        <el-input v-model="form.logTopic" placeholder="请输入Log-topic" clearable> </el-input>
      </el-form-item>
      <el-form-item label="ClassName:" style="width: 66%" prop="className">
        <el-input v-model="form.className" placeholder="请输入ClassName" clearable> </el-input>
      </el-form-item>
      <el-form-item label="Runtime:" style="width: 33%; margin-left: 5px" prop="runtime">
        <el-select v-model="form.runtime" style="width: 100%">
          <el-option label="JAVA" value="JAVA"> </el-option>
          <el-option label="GO" value="GO"> </el-option>
        </el-select>
      </el-form-item>
      <el-form-item class="upload" label="File:" prop="file" style="width: 100%">
        <el-upload ref="fileToUpload" drag :limit="1" accept=".jar" action=""
:auto-upload="false" :on-change="getFile">
          <i class="el-icon-upload"></i>
          <div class="el-upload__text">将文件拖到此处，或点击上传</div>
          <div class="el-upload__tip">目前只支持jar文件上传</div>
        </el-upload>
      </el-form-item>
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
          zIndex: 1
        }"
      >
        <el-button type="primary" @click="onSubmit">立即创建</el-button>
        <el-button @click="resetField()">重置</el-button>
      </div>
    </el-form>
  </el-drawer>
</template>

<script>
  import { create } from '@/api/func'
  import { reactive, ref } from '@vue/runtime-core'
  import { ElMessage, ElMessageBox } from 'element-plus'
  export default {
    name: 'AddFunc',
    emits: ['onRefresh'],
    setup(props, context) {
      const addForm = ref(null)
      const addFuncDrawer = ref(null)
      const fileToUpload = ref(null)
      const file = reactive({})
      const form = reactive({
        functionName: '',
        inputs: [''],
        output: '',
        logTopic: '',
        className: '',
        runtime: 'JAVA'
      })
      const rules = {
        functionName: [{ required: true, message: '请输入FunctionName', trigger: 'blur' }],
        output: [{ required: true, message: '请输入Output', trigger: 'blur' }],
        logTopic: [{ required: true, message: '请输入Log-topic', trigger: 'blur' }],
        className: [{ required: true, message: '请输入ClassName', trigger: 'blur' }]
      }
      const onSubmit = () => {
        addForm.value.validate(async (valid) => {
          if (!valid) return
          const data = new FormData()
          if (!file.value) {
            ElMessage.error('Need files to upload')
            return
          } else {
            const fileName = file.value.name
            if (fileName.substr(fileName.lastIndexOf('.') + 1) !== 'jar') {
              ElMessage.error('Only jar files are supported, please check the type of your uploaded file.')
              return
            } else {
              data.append('data', file.value)
            }
          }
          const functionConfig = JSON.parse(JSON.stringify(form).replace(/logTopic/g, 'log-topic'))
          const functionName = functionConfig.functionName
          delete functionConfig.functionName
          data.append('functionConfig', new Blob([JSON.stringify(functionConfig)], { type: 'application/json' }))
          creationSubmit(data, functionName)
        })
      }
      //Network request
      const creationSubmit = (data, functionName) => {
        ElMessageBox.confirm('即将添加函数', '提示', {
          confirmButtonText: '确定',
          cancelButtonText: '取消',
          type: 'warning'
        })
          .then(() => {
            create(functionName, data).then(() => {
              ElMessage({
                type: 'success',
                message: '添加成功'
              })
              onClose()
              context.emit('onRefresh') //刷新table上的数据
            })
          })
          .catch((err) => {
            let errMessage = ''
            if (err.response) {
              errMessage = err.response.data.reason
            }
            ElMessage({
              type: 'info',
              message: ` funciton creation failed, because ${errMessage}`
            })
          })
      }
      const onClose = () => {
        resetField()
        addFuncDrawer.value.handleClose() //关闭drawer
      }
      //add and delete inputs
      const addInput = () => {
        form.inputs.push('')
      }
      const deleteInput = (index) => {
        form.inputs.splice(index, 1)
      }
      //restAllField
      const resetField = () => {
        addForm.value.resetFields() //清空已输入的数据
        file.value = '' //清空文件
        form.inputs = ['']
        fileToUpload.value.clearFiles() //清空已上传文件列表
      }
      //get uploaded file
      const getFile = (fileToUp) => {
        file.value = fileToUp.raw
      }
      return {
        getFile,
        resetField,
        deleteInput,
        addInput,
        onSubmit,
        onClose,
        creationSubmit,
        rules,
        form,
        file,
        fileToUpload,
        addForm,
        addFuncDrawer
      }
    }
  }
</script>
<style scoped>
  .topic {
    line-height: 32px;
    display: block;
    text-align: left;
  }
  .upload ::v-deep(.el-input) {
    border: black;
  }
  .upload ::v-deep(.el-upload) {
    width: 100%;
  }
  .upload ::v-deep(.el-upload-dragger) {
    width: 100%;
  }
  .upload ::v-deep(.el-upload__tip) {
    margin: 0;
  }
  .el-form-item {
    margin-top: 5px;
    margin-right: 0;
  }
  .el-form-item--mini.el-form-item,
  .el-form-item--small.el-form-item {
    margin-bottom: 0;
  }
  .addFunc ::v-deep(.el-form-item__label) {
    padding: 0;
  }
  .addFunc ::v-deep(.el-form-item__error) {
    position: relative;
    float: left;
  }
  .addInputIcon {
    font-size: 1.5em;
    width: 5%;
    text-align: center;
    vertical-align: middle;
    padding-left: 0.1em;
  }
  .addInputIcon:hover {
    cursor: pointer;
    color: #40a3ff;
  }
  .el-drawer ::v-deep(.el-drawer__body) {
    overflow: auto !important;
  }
</style>
