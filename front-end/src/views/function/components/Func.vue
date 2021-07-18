<template>
  <el-table :data="data" style="width: 100%">
    <el-table-column :label="$t('func.table.Name')" prop="name" />
    <el-table-column :label="$t('func.table.Status')" prop="status">
      <template #default="scope">
        <div class="flex items-center select-none">
          <el-tag
            v-if="typeof scope.row.status === 'boolean'"
            :type="scope.row.status ? 'success' : 'info'"
            effect="dark"
          >
            {{ scope.row.status ? 'Running' : 'Down' }}
          </el-tag>
          <i v-else class="el-icon-loading text-blue-500 text-lg" />
        </div>
      </template>
    </el-table-column>
    <el-table-column :label="$t('func.table.Options')" prop="options">
      <template #default="scope">
        <div class="text-blue-500 cursor-pointer select-none">
          <a class="inline-block" @click="onSelFunction(scope.row)">Trigger</a>
          <el-divider direction="vertical" />
          <a class="inline-block" @click="onShowDetail(scope.row)">Detail</a>
          <el-divider direction="vertical" />
          <a v-show="!scope.row.status" class="inline-block" @click="onStart(scope.row)">Start</a>
          <a v-show="scope.row.status" class="inline-block" @click="onStop(scope.row)">Stop</a>
          <el-divider direction="vertical" />
          <a class="inline-block" @click="onDelete(scope.row)">Delete</a>
        </div>
      </template>
    </el-table-column>
    <template #empty>
      <el-empty :description="$t('comp.emptyData')"></el-empty>
    </template>
  </el-table>
</template>

<script>
  import { deleteFunc, startFunc, stopFunc } from '@/api/func'

  export default {
    props: {
      data: {
        type: Array,
        default: function () {
          return []
        }
      },
      visibleTrigger: {
        type: Boolean,
        default: false
      },
      onSelFunction: {
        type: Function,
        default: (v) => {}
      },
      onRreshFunc: {
        type: Function,
        default: () => {}
      },
      visibleDetail: {
        type: Boolean,
        default: false
      },
      onShowDetail: {
        type: Function,
        default: (v) => {}
      }
    },
    methods: {
      onDelete(func) {
        const { name = '' } = func
        this.$confirm(this.$t('func.deleteFuncNotice', [name]), 'comp.noice', {
          type: 'warning'
        })
          .then(async () => {
            try {
              await deleteFunc(name)
              this.$store.dispatch('app/onRefresh')
              this.$message.success(this.$t('func.deleteSuccess', [name]))
            } catch (error) {
              this.$message.info(this.$t('func.deleteFail', [name]))
            }
          })
          .catch(() => {
            this.$message.info(this.$t('func.cancelDelete', [name]))
          })
      },
      onStart(text) {
        const { name = '' } = text
        const Start = async () => {
          try {
            const res = await startFunc(name)
            text.status = true
            this.$message.success(this.$t('func.startSuccess'))
          } catch (error) {
            this.$message.error(this.$t('func.startFail'))
            console.error(error)
          }
        }
        Start()
      },
      onStop(text) {
        const { name = '' } = text
        const Stop = async () => {
          try {
            await stopFunc(name)
            text.status = false
            this.$message.success(this.$t('func.stopSuccess'))
          } catch (error) {
            this.$message.error(this.$t('func.stopFail'))
            console.error(error)
          }
        }
        Stop()
      }
    }
  }
</script>
