<template>
  <div>
    <page-header-wrapper>
      <template slot="extra">
        <div class="table-operator">
          <a-button type="primary"
                    icon="plus"
                    @click="showDrawer">Add Function</a-button>
        </div>
      </template>

      <a-card :bordered="false">
        <function-table :data="functionList"
                        :loadingList="loadingList"
                        :onSelFunction="onSelFunction"
                        :onShowDetail="onShowDetail"
                        :onRefreshFunc="onRefreshFunc" />
      </a-card>
    </page-header-wrapper>

    <!-- new funtion -->
    <add-form :visible="visibleDrawer" />
    <!-- trigger -->
    <trigger :visible="visibleTrigger"
             :data="functionList"
             :currentFunction="currentFunction" />
    <!-- detail -->
    <function-detail-vue :visible="visibleDetail"
                         :currentFuncionInfo="currentFuncionInfo"
                         :loadingDetail="loadingDetail" />
  </div>
</template>
<script>
import AddForm from './components/AddForm.vue'
import FunctionDetailVue from './components/FunctionDetail.vue'
import FunctionTable from './components/FunctionTable'
import Trigger from './components/Trigger.vue'
import { getList, getStatus, getInfo, getStats } from '@/api/func'
import moment from 'moment'

export default {
  data () {
    return {
      functionList: [],
      visibleDrawer: false,
      visibleTrigger: false,
      visibleDetail: false,
      currentFunction: {},
      currentFuncionInfo: {},
      loadingList: false,
      loadingDetail: false
    }
  },
  components: {
    FunctionTable,
    AddForm,
    Trigger,
    FunctionDetailVue
  },
  mounted () {
    this.refreshFunc()
  },
  methods: {
    async refresh () {
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
      } catch (e) { }
      this.loadingList = false
    },
    formatDate (date) {
      return moment(date).format('YYYY/MM/DD')
    },
    showDrawer () {
      this.visibleDrawer = true
    },
    closeDrawer () {
      this.visibleDrawer = false
    },
    showTrigger () {
      this.visibleTrigger = true
    },
    closeTrigger () {
      this.visibleTrigger = false
    },
    showDetail () {
      this.visibleDetail = true
    },
    closeDetail () {
      this.visibleDetail = false
    },
    onSelFunction (value) {
      this.currentFunction = value
      this.showTrigger()
    },
    onShowDetail (value) {
      this.loadingDetail = true
      this.currentFuncionInfo = { ...this.currentFuncionInfo, ...value }
      this.showDetail()
      const { name } = value
      getInfo(name).then((res) => {
        if (!res) return
        const { inputSpecs = {} } = res
        const input = Object.keys(inputSpecs)
        this.currentFuncionInfo = { ...this.currentFuncionInfo, ...res, input }
      }).finally(() => {
        this.loadingDetail = false
      })
      getStats(name).then((res) => {
        if (!res) return
        this.currentFuncionInfo = { ...this.currentFuncionInfo, ...res }
      }).finally(() => {
        this.loadingDetail = false
      })
    },
    async refreshFunc () {
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
      } catch (e) { }
      this.loadingList = false
    },
    onRefreshFunc () {
      this.refreshFunc()
    }
  }
}
</script>
