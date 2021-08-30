<template>
  <el-drawer v-model="drawer" :size="720" :title="$t('funcHub.addFunc')" direction="rtl" :before-close="handleClose">
    <el-form ref="formRef" class="form" :model="form" label-position="top" :rules="rules">
      <el-form-item class="form-item" :label="$t('funcHub.form.functionName')" prop="name">
        <el-input v-model="form.name" clearable></el-input>
      </el-form-item>
      <el-form-item class="form-item" :label="$t('funcHub.form.uploadFile')" :required="true">
        <el-upload class="upload" drag action="" multiple width="100%">
          <i class="el-icon-upload"></i>
          <div class="el-upload__text">
            {{ $t('funcHub.form.uploadText') }}<em>{{ $t('funcHub.form.uploadClick') }}</em>
          </div>
          <div class="upload-tip">
            {{ $t('funcHub.form.uploadSupport') }}
          </div>
        </el-upload>
      </el-form-item>
    </el-form>
    <div class="operate-btn">
      <el-button @click="handleReset">{{ $t('funcHub.optionsBtn.reset') }}</el-button>
      <el-button type="primary">{{ $t('funcHub.optionsBtn.submit') }}</el-button>
    </div>
  </el-drawer>
</template>

<script>
  import { defineComponent, computed, reactive, ref, getCurrentInstance } from 'vue'

  export default defineComponent({
    name: 'AddFunc',
    props: {
      drawerFlag: {
        type: Boolean,
        default: false
      }
    },
    emits: ['on-close'],
    setup(props, { emit }) {
      const instance = getCurrentInstance()
      const { $t } = instance.appContext.config.globalProperties

      const rules = {
        name: [{ required: true, message: $t('funcHub.form.nameNotice'), trigger: 'blur' }]
      }
      const form = reactive({
        name: ''
      })
      const formRef = ref(null)

      const drawer = computed(() => {
        return props.drawerFlag
      })

      const handleClose = () => {
        emit('on-close')
      }

      const handleReset = () => {
        formRef.value.resetFields()
      }
      return {
        rules,
        form,
        drawer,
        formRef,
        handleClose,
        handleReset
      }
    }
  })
</script>

<style scoped>
  .form {
    padding: 0 40px;
  }
  .form-item ::v-deep(.el-form-item__label) {
    padding: 0;
  }
  .upload ::v-deep(.el-upload) {
    width: 100%;
  }
  .upload ::v-deep(.el-upload-dragger) {
    width: 100%;
  }
  .upload-tip {
    color: rgba(0, 0, 0, 0.45);
  }
  .operate-btn {
    position: absolute;
    bottom: 0;
    border-top: 1px solid #e9e9e9;
    width: 100%;
    padding: 10px 16px;
    text-align: right;
  }
</style>
