<template>
  <el-table :data="data" style="width: 100%">
    <el-table-column :label="$t('funcHub.table.Name')" prop="name" />
    <el-table-column :label="$t('funcHub.table.Options')" prop="options">
      <template #default="scope">
        <div class="text-blue-500 cursor-pointer select-none">
          <a class="inline-block" @click="handleDetail(scope.row)">{{ $t('funcHub.optionsBtn.detail') }}</a>
          <el-divider direction="vertical" />
          <a class="inline-block" @click="hanleDelete(scope.row)">{{ $t('funcHub.optionsBtn.delete') }}</a>
        </div>
      </template>
    </el-table-column>
    <template #empty>
      <el-empty :description="$t('comp.emptyData')"></el-empty>
    </template>
  </el-table>
</template>

<script>
  import { getCurrentInstance, defineComponent } from 'vue'

  export default defineComponent({
    name: 'FuncTable',
    props: {
      data: {
        type: Array,
        default: function () {
          return []
        }
      }
    },
    emits: ['on-show-detail'],
    setup(props, { emit }) {
      const instance = getCurrentInstance()
      const { $confirm, $t, $message } = instance.appContext.config.globalProperties

      const hanleDelete = (func) => {
        const { name = '' } = func
        $confirm($t('funcHub.notice.deleteFuncNotice', [name]), $t('comp.notice'), {
          type: 'warning'
        })
          .then(async () => {
            try {
              // TODO
              // delete api
              $message.success($t('funcHub.notice.deleteSuccess', [name]))
            } catch (error) {
              $message.info($t('funcHub.notice.deleteFail', [name]))
            }
          })
          .catch(() => {
            $message.info($t('funcHub.notice.cancelDelete', [name]))
          })
      }
      const handleDetail = () => {
        emit('on-show-detail')
      }

      return {
        hanleDelete,
        handleDetail
      }
    }
  })
</script>
