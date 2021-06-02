<template>
  <a-table :columns="columns" :loading="loadingList" :data-source="data">
    <span slot="status" slot-scope="text">
      <a-badge v-if="text === true" status="processing" text="Running" />
      <a-badge v-else-if="text === false" status="default" text="Down" />
      <a-spin v-else size="small" />
    </span>

    <span slot="action" slot-scope="text">
      <a @click="onSelFunction(text)">Trigger</a>
      <a-divider type="vertical" />
      <a @click="onShowDetail(text)">Detail</a>
      <a-divider type="vertical" />
      <a @click="onDelete(text)">Delete</a>
    </span>
  </a-table>
</template>

<script>
// import moment from 'moment';
import { deleteFunc } from '@/api/func'

const columns = [
  {
    title: 'Name',
    dataIndex: 'name',
    key: 'name'
  },
  {
    title: 'Status',
    dataIndex: 'status',
    key: 'status',
    scopedSlots: { customRender: 'status' }
  },
  {
    title: 'Options',
    key: 'action',
    scopedSlots: { customRender: 'action' }
  }
]

export default {
  data () {
    return {
      columns
    }
  },
  props: {
    data: {
      type: Array,
      default: []
    },
    onSelFunction: {
      type: Function,
      default: (v) => {}
    },
    onShowDetail: {
      type: Function,
      default: (v) => {}
    },
    loadingList: {
      type: Boolean,
      default: false
    }
  },
  methods: {
    onDelete (func) {
      const { name = '' } = func
      const _this = this
      this.$confirm({
        title: 'Are you sure delete this function?',
        content: 'Some descriptions',
        okType: 'danger',
        async onOk () {
          try {
            const res = await deleteFunc(name)
            _this.$notification.success({ message: `"${name}" function deleted successfully` })
          } catch (error) {
            _this.$notification.error({ message: `"${name}" funciton deletion failed` })
          }
        }
      })
    }
  }
}
</script>
