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
    />
    <TriggerVue v-model="visibleTrigger" :data="functionList" :currentFunction="currentFunction" />
  </PageHeaderWrapper>
</template>
<script>
  import Func from './components/Func.vue'
  import TriggerVue from './components/trigger.vue'
  import { getList, getStatus, getInfo, getStats } from '@/api/func'
  export default {
    components: {
      Func,
      TriggerVue
    },
    data() {
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
    }
  }
</script>

<style></style>
