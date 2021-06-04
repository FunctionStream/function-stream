<template>
  <a-table :columns="columns" :loading="loadingList" :data-source="data">
    <span slot="status" slot-scope="text">
      <a-badge v-if="text === true" v-model="text" status="processing" text="Running" />
      <a-badge v-else-if="text === false" v-model="text" status="default" text="Down" />
      <a-spin v-else size="small" />
    </span>

    <span slot="action" slot-scope="text">
      <a @click="onSelFunction(text)">Trigger</a>
      <a-divider type="vertical" />
      <a @click="onStart(text)" v-show="!text.status">Start</a>
      <a @click="onStop(text)" v-show="text.status">Stop</a>
      <a-divider type="vertical" />
      <a @click="onShowDetail(text)">Detail</a>
      <a-divider type="vertical" />
      <a @click="onDelete(text)">Delete</a>
    </span>
  </a-table>
</template>

<script>
// import moment from 'moment';
import { deleteFunc, startFunc } from '@/api/func'

const columns = [
  {
    title: 'Name',
    dataIndex: 'name',
    key: 'name',
    width: '33.3%',
  },
  {
    title: 'Status',
    dataIndex: 'status',
    key: 'status',
    width: '33.3%',
    scopedSlots: { customRender: 'status' },
  },
  {
    title: 'Options',
    key: 'action',
    width: '33.3%',
    scopedSlots: { customRender: 'action' },
  },
]

export default {
  data() {
    return {
      columns,
      isShow: false
    }
  },
  props: {
    data: {
      type: Array,
      default: [],
    },
    onSelFunction: {
      type: Function,
      default: (v) => {},
    },
    onShowDetail: {
      type: Function,
      default: (v) => {},
    },
    loadingList: {
      type: Boolean,
      default: false,
    },
  },
  methods: {
    onDelete(func) {
      const { name = '' } = func
      const _this = this
      this.$confirm({
        title: 'Are you sure delete this function?',
        content: 'Some descriptions',
        okType: 'danger',
        async onOk() {
          try {
            const res = await deleteFunc(name)
            _this.$notification.success({ message: `"${name}" function deleted successfully` })
          } catch (error) {
            _this.$notification.error({ message: `"${name}" funciton deletion failed` })
          }
        },
      })
    },
    onStart(text) {
      const { name = '' } = text
      const _this = this
      async function Start() {
        try {
          const res = await startFunc(name)
          text.status = !text.status
        } catch (error) {
          console.error(res)
        }
      }
      Start()
    },
    onStop(text) {
      const { name = '' } = text
      const _this = this
      async function Stop() {
        try {
          const res = await startFunc(name)
          text.status = !text.status
        } catch (error) {
          console.error(res)
        }
      }
      Stop()
    },
  },
}
</script>
