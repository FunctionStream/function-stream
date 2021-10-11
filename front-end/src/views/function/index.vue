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
      :onSelFunction="onSelFunction"
      :loadingList="loadingList"
      :onRefreshFunc="onRreshFunc"
      :onShowDetail="onShowDetail"
    />
    <TriggerVue v-model="visibleTrigger" :data="functionList" :currentFunction="currentFunction" />
    <FunctionDetailVue
      v-model="visibleDetail"
      :currentFunctionInfo="currentFunctionInfo"
      :loadingDetail="loadingDetail"
    />
    <add-func v-model="visibleAdd" @onRefresh="onRefreshFunc" />
  </PageHeaderWrapper>
</template>
<script>
  import Func from './components/Func.vue'
  import TriggerVue from './components/trigger.vue'
  import { reactive, ref } from '@vue/runtime-core'
  import { getList, getStatus, getInfo, getStats } from '@/api/func'
  import AddFunc from './components/AddFunc.vue'
  import FunctionDetailVue from './components/FunctionDetail'
  export default {
    components: {
      Func,
      TriggerVue,
      FunctionDetailVue,
      AddFunc
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

      //trigger
      /* functionList: [],
        loading: false,
        visibleTrigger: false,
        visibleDetail: false,
        currentFunction: {},
        currentFunctionInfo: {},
        loadingDetail: false,
        loadingList: false */
      const visibleTrigger = ref(false)
      const currentFunction = ref(null)
      /* showTrigger() {
        this.visibleTrigger = true
      },
      closeTrigger() {
        this.visibleTrigger = false
      },
      onSelFunction(value) {
        this.currentFunction = value
        this.showTrigger()
      }, */
      const showTrigger = () => {
        visibleTrigger.value = true
      }
      const closeTrigger = () => {
        visibleTrigger.value = false
      }
      const onSelFunction = (v) => {
        currentFunction.value = v
        showTrigger()
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
        closeDrawer,
        visibleTrigger,
        currentFunction,
        showTrigger,
        closeTrigger,
        onSelFunction
      }
    },
    data() {
      return {
        visibleAdd: false
      }
    },
    created() {
      this.refresh()
    },
    methods: {
      async refresh() {
        this.loadingList = true
        try {
          const res = await getList()
          if (Array.isArray(res)) {
            this.functionList = res?.map((name) => ({ key: name, name }))
            // get status
            // fixme this use of map should be replaced ↓↓
            // eslint-disable-next-line no-unused-expressions
            res?.map(async (name, i) => {
              const res = await getStatus(name)
              this.functionList[i].status = !!res?.instances?.[0]?.status?.running
              this.functionList[i].statusInfo = res
            })
          }
        } catch (e) {}
        this.loadingList = false
      },
      showAddFunc() {
        this.visibleAdd = true
      },
      async refreshFunc() {
        const _this = this
        this.loadingList = true
        try {
          const res = await getList()
          if (Array.isArray(res)) {
            _this.functionList = res?.map((name) => ({ key: name, name }))
            // get status
            // eslint-disable-next-line no-unused-expressions
            res?.map(async (name, i) => {
              const res = await getStatus(name)
              _this.$set(_this.functionList[i], 'status', !!res?.instances?.[0]?.status?.running)
              _this.$set(_this.functionList[i], 'statusInfo', res)
            })
          }
        } catch (e) {}
        this.loadingList = false
      },
      onRreshFunc() {
        this.refreshFunc()
      }
    }

    /* data() {
      return {
        functionList: [],
        loading: false,
        visibleTrigger: false,
        visibleDetail: false,
        currentFunction: {},
        currentFunctionInfo: {},
        loadingDetail: false,
        loadingList: false
      }
    },
    created() {
      this.refresh()
    },
    methods: {
      async refresh() {
        this.loadingList = true
        try {
          const res = await getList()
          if (Array.isArray(res)) {
            this.functionList = res?.map((name) => ({ key: name, name }))
            // get status
            // fixme this use of map should be replaced ↓↓
            // eslint-disable-next-line no-unused-expressions
            res?.map(async (name, i) => {
              const res = await getStatus(name)
              this.$set(this.functionList[i], 'status', !!res?.instances?.[0]?.status?.running)
              this.$set(this.functionList[i], 'statusInfo', res)
            })
          }
        } catch (e) {}
        this.loadingList = false
      },
      showAddFunc() {
        this.visibleAdd = true
      },
      showTrigger() {
        this.visibleTrigger = true
      },
      closeTrigger() {
        this.visibleTrigger = false
      },
      onSelFunction(value) {
        this.currentFunction = value
        this.showTrigger()
      },
      closeDetail() {
        this.visibleDetail = false
      },
      showDetail() {
        this.visibleDetail = true
      },
      onShowDetail(v) {
        console.log('v in index', v)
        this.loadingDetail = true
        this.currentFunctionInfo = { ...this.currentFunctionInfo, ...v }
        this.showDetail()
        const { name } = v
        getInfo(name)
          .then((res) => {
            if (!res) return
            const { inputSpecs = {} } = res
            const input = Object.keys(inputSpecs)
            this.currentFunctionInfo = { ...this.currentFunctionInfo, ...res, input }
          })
          .finally(() => {
            this.loadingDetail = false
          })
        getStats(name)
          .then((res) => {
            if (!res) return
            this.currentFunctionInfo = { ...this.currentFunctionInfo, ...res }
          })
          .finally(() => {
            this.loadingDetail = false
          })
      },
      async refreshFunc() {
        const _this = this
        this.loadingList = true
        try {
          const res = await getList()
          if (Array.isArray(res)) {
            _this.functionList = res?.map((name) => ({ key: name, name }))
            // get status
            // eslint-disable-next-line no-unused-expressions
            res?.map(async (name, i) => {
              const res = await getStatus(name)
              _this.$set(_this.functionList[i], 'status', !!res?.instances?.[0]?.status?.running)
              _this.$set(_this.functionList[i], 'statusInfo', res)
            })
          }
        } catch (e) {}
        this.loadingList = false
      },
      onRreshFunc() {
        this.refreshFunc()
      }
    } */
  }
</script>

<style></style>
