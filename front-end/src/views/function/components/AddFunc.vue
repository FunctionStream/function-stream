<template>
  <el-drawer ref="addFuncDrawer" size="40%" title="AddFunction">
    <el-form
      ref="addForm"
      class="addFunc"
      :rules="rules"
      :model="form"
      label-width="80px"
      label-position="top"
      style="width: 90%; margin: 25px"
      size="small"
      :inline="true"
    >
      <el-form-item label="FunctionName:" style="width: 100%" prop="functionName">
        <el-input v-model="form.functionName" placeholder="请输入FunctionName" clearable> </el-input>
      </el-form-item>
      <a style="color: red; font-size: 14px"> *&nbsp; </a>
      <a style="font-size: 14px"> Inputs: </a>
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
        <el-input v-model="form.inputs[index]" style="width: 95%" placeholder="请输入Input" clearable> </el-input>
        <i
          v-show="form.inputs.length !== 1"
          style="float: right; font-size: 30px; width: 5%"
          class="el-icon-circle-close"
          @click="deleteInput(index)"
        >
        </i>
      </el-form-item>
      <el-button
        style="width: 100%; font-size: 20px; margin-top: 5px"
        size="mini"
        class="el-icon-circle-plus-outline"
        @click="addInput"
      >
      </el-button>
      <el-form-item label="Output:" style="width: 100%" prop="output">
        <el-input v-model="form.output" placeholder="请输入Output" clearable> </el-input>
      </el-form-item>
      <el-form-item label="Log-topic:" style="width: 100%" prop="logTopic">
        <el-input v-model="form.logTopic" placeholder="请输入Log-topic" clearable> </el-input>
      </el-form-item>
      <el-form-item label="ClassName:" style="width: 66%" prop="className">
        <el-input v-model="form.className" placeholder="请输入ClassName" clearable> </el-input>
      </el-form-item>
      <el-form-item label="Runtime:" style="width: 30%" prop="runtime">
        <el-select v-model="form.runtime">
          <el-option label="JAVA" value="JAVA"> </el-option>
          <el-option label="GO" value="GO"> </el-option>
        </el-select>
      </el-form-item>
      <el-form-item class="upload" label="File:" prop="file" style="width: 100%">
        <el-upload
          ref="file"
          :limit="1"
          drag
          accept=".jar"
          action="0.0//不写会报错，神奇"
          :auto-upload="false"
          :on-change="getFile"
        >
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
        <el-button @click="resetField('addForm')">重置</el-button>
      </div>
    </el-form>
  </el-drawer>
</template>

<script>
  import { create } from '@/api/func'
  export default {
    props: {
      functionList: {
        type: Object,
        default: () => {}
      },
      refresh: {
        type: Function,
        default: () => {}
      }
    },
    data() {
      return {
        file: '',
        form: {
          functionName: '',
          inputs: [''],
          output: '',
          logTopic: '',
          className: '',
          runtime: 'JAVA'
        },
        rules: {
          functionName: [{ required: true, message: '请输入FunctionName', trigger: 'blur' }],
          output: [{ required: true, message: '请输入Output', trigger: 'blur' }],
          logTopic: [{ required: true, message: '请输入Log-topic', trigger: 'blur' }],
          className: [{ required: true, message: '请输入ClassName', trigger: 'blur' }]
        }
      }
    },
    methods: {
      onSubmit() {
        this.$refs.addForm.validate(async (valid) => {
          if (!valid) return
          if (!this.file) {
            this.$message('还没添加文件')
            return
          }
          const functionConfig = JSON.parse(JSON.stringify(this.form).replace(/logTopic/g, 'log-topic'))
          const functionName = functionConfig.FunctionName
          delete functionConfig.functionName
          console.log(functionConfig)
          const data = new FormData()
          data.append('data', this.file)
          data.append('functionConfig', new Blob([JSON.stringify(functionConfig)], { type: 'application/json' }))
          console.log(data)
          this.$confirm('即将添加函数', '提示', {
            confirmButtonText: '确定',
            cancelButtonText: '取消',
            type: 'warning'
          })
            .then(() => {
              create(functionName, data).then((res) => {
                this.$notify({
                  type: 'success',
                  message: '添加成功'
                })
                this.$refs.addForm.resetFields() //清空已输入的数据
                this.file = '' //清空文件
                this.$refs.file.clearFiles() //清空已上传文件列表
                this.$refs.addFuncDrawer.handleClose() //关闭drawer
                this.refresh() //刷新table上的数据
              })
            })
            .catch(() => {
              this.$notify({
                type: 'info',
                message: '添加失败'
              })
            })
        })
        // console.log(JSON.parse(JSON.stringify(this.form)))
      },
      addInput() {
        this.form.inputs.push('')
      },
      deleteInput(index) {
        this.form.inputs.splice(index, 1)
      },
      resetField(formName) {
        this.$refs[formName].resetFields()
      },
      getFile(file) {
        this.file = file.raw
      }
    }
  }
</script>
<style scoped>
  .upload ::v-deep(.el-upload) {
    width: 100%;
  }
  .upload ::v-deep(.el-upload-dragger) {
    width: 100%;
  }
  .el-form-item--mini.el-form-item,
  .el-form-item--small.el-form-item {
    margin-bottom: 0px;
  }
  .addFunc ::v-deep(.el-form-item__error) {
    position: relative;
    float: left;
  }
  .el-drawer ::v-deep(.el-drawer__body) {
    overflow: auto !important;
  }
  /*.el-drawer rtl ::v-deep(.el-drawer__container ::-webkit-scrollbar){*/
  /*  display: none;*/
  /*}*/
</style>
