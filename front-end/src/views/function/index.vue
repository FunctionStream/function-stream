<template>
  <PageHeaderWrapper>
    <template #extra>
      <el-button icon="el-icon-circle-plus-outline" type="primary" class="mr-4" @click="showAddFunc">
        {{ $t('func.addFunc') }}
      </el-button>
    </template>
    <Func
      v-loading="loading"
      :data="functionList"
      :onShowDetail="onShowDetail"
      :loadingList="loadingList"
      :onRefreshFunc="onRefreshFunc"
    />
    <FunctionDetailVue
      v-model="visibleDetail"
      :currentFunctionInfo="currentFunctionInfo"
      :loadingDetail="loadingDetail"
    />
    <add-func v-model="visibleAdd" :functionList="functionList" :refresh="refresh" />
  </PageHeaderWrapper>
</template>
<script>
  import Func from './components/Func.vue'
  import AddFunc from './components/AddFunc.vue'
  import FunctionDetailVue from './components/FunctionDetail'
  import { getList, getStatus, getInfo, getStats } from '@/api/func'
  import { ref } from '@vue/reactivity'
  export default {
    components: {
      FunctionDetailVue,
      AddFunc,
      Func
    },
    setup() {
      // get function list
      const functionList = ref([])
      const getFunctionList = async () => {
        const res = await getList()
        if (Array.isArray(res)) {
          functionList.value = res?.map((name) => ({ key: name, name }))
          // get status
          res?.map(async (name, i) => {
            const res = await getStatus(name)
            functionList.value[i]['status'] = !!res?.numRunning
            functionList.value[i]['statusInfo'] = res
          })
        }
      }

      const loading = ref(true)
      const initFuncList = () => {
        loading.value = true
        getFunctionList().then(() => {
          loading.value = false
        })
      }
      initFuncList()

      // function detail
      const visibleDetail = ref(false)
      const loadingDetail = ref(false)
      const currentFunctionInfo = ref(null)
      const closeDetail = () => {
        visibleDetail.value = false
      }
      const showDetail = () => {
        visibleDetail.value = true
      }
      const onShowDetail = (v) => {
        loadingDetail.value = true
        currentFunctionInfo.value = { ...currentFunctionInfo.value, ...v }
        showDetail()
        const { name } = v
        getInfo(name)
          .then((res) => {
            if (!res) return
            const { inputSpecs = {} } = res
            const input = Object.keys(inputSpecs)
            currentFunctionInfo.value = { ...currentFunctionInfo.value, ...res, input }
          })
          .finally(() => {
            loadingDetail.value = false
          })
        getStats(name)
          .then((res) => {
            if (!res) return
            currentFunctionInfo.value = { ...currentFunctionInfo.value, ...res }
          })
          .finally(() => {
            loadingDetail.value = false
          })
      }

      // add function
      const visibleAdd = ref(false)
      const showAddFunc = () => {
        visibleAdd.value = true
      }
      
      // refresh function
      const loadingList = ref(false)
      const onRefreshFunc = () => {
        loadingList.value = true
        getFunctionList.then(() => {
          loadingList.value = false
        })
      }

      const closeDrawer = () => {
        currentFunctionInfo.value = {}
        closeDetail()
      }

      return {
        loading,
        functionList,
        loadingList,
        loadingDetail,
        visibleDetail,
        currentFunctionInfo,
        closeDetail,
        showDetail,
        onShowDetail,
        onRefreshFunc,
        closeDrawer
      }
    }
  }
</script>
