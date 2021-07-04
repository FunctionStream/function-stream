<template>
  <PageHeaderWrapper>
    <template #extra>
      <el-button icon="el-icon-circle-plus-outline" type="primary" class="mr-4">
        {{ $t('func.addFunc') }}
      </el-button>
    </template>
    <Func
      v-loading="loading"
      :data="functionList"
      :onShowDetail="onShowDetail"
      :loadingList="loadingList"
      :onRefreshFunc="onRreshFunc"
    />
    <FunctionDetailVue
      v-model="visibleDetail"
      :currentFunctionInfo="currentFunctionInfo"
      :loadingDetail="loadingDetail"
    />
  </PageHeaderWrapper>
</template>
<script>
  import Func from './components/Func.vue'
  import FunctionDetailVue from './components/FunctionDetail'
  import { getList, getStatus, getInfo, getStats } from '@/api/func'
  export default {
    components: {
      FunctionDetailVue,
      Func
    },
    data() {
      return {
        functionList: [],
        loading: false,
        visibleDetail: false,
        currentFunctionInfo: {},
        loadingDetail: false,
        loadingList: false
      }
    },
    async created() {
      try {
        this.loading = true
        const res = await getList()
        if (Array.isArray(res)) {
          this.functionList = res?.map((name) => ({ key: name, name }))
          // get status
          res?.map(async (name, i) => {
            const res = await getStatus(name)
            this.functionList[i]['status'] = !!res?.numRunning
            this.functionList[i]['statusInfo'] = res
          })
        }
      } catch (e) {}
      this.loading = false
    },
    methods: {
      closeDetail() {
        this.visibleDetail = false
      },
      showDetail() {
        this.visibleDetail = true
      },
      onShowDetail(v) {
        this.loadingDetail = true
        this.currentFunctionInfo = { ...this.currentFunctionInfo, ...v }
        this.showDetail()
        const { name } = v
        getInfo(v)
          .then((res) => {
            if (!res) return
            const { inputSpaces = {} } = res
            const input = Object.keys(inputSpaces)
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
            console.log('cfi in onshowdetail', this.currentFunctionInfo)
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
    }
  }
</script>
