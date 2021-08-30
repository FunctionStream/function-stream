<template>
  <PageHeaderWrapper>
    <template #extra>
      <el-button icon="el-icon-circle-plus-outline" type="primary" class="mr-4" @click="handleAddFunc">
        {{ $t('funcHub.addFunc') }}
      </el-button>
    </template>
    <FuncTable v-loading="loading" :data="functionList" @on-show-detail="handleShowDetail" />
    <AddFunc :drawerFlag="drawerFlag" @on-close="handleCloseAddFunc" />
  </PageHeaderWrapper>
</template>
<script>
  import { defineComponent, ref } from 'vue'
  import FuncTable from './components/FuncTable.vue'
  import AddFunc from './components/AddFunc.vue'
  export default defineComponent({
    components: {
      FuncTable,
      AddFunc
    },
    setup(props) {
      const functionList = [
        {
          id: 1,
          name: 'function1'
        },
        {
          id: 2,
          name: 'function2'
        }
      ]
      const loading = ref(false)
      const drawerFlag = ref(false)
      const getTableData = async () => {
        // TODO
        // get data api
      }

      getTableData()

      const handleAddFunc = () => {
        drawerFlag.value = true
      }

      const handleCloseAddFunc = () => {
        drawerFlag.value = false
      }

      const handleShowDetail = () => {}
      return {
        functionList,
        loading,
        drawerFlag,
        handleAddFunc,
        handleCloseAddFunc,
        handleShowDetail
      }
    }
  })
</script>
