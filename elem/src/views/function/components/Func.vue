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
            {{ scope.row.status ? "Running" : "Down" }}
          </el-tag>
          <i v-else class="el-icon-loading text-blue-500 text-lg" />
        </div>
      </template>
    </el-table-column>
    <el-table-column :label="$t('func.table.Options')" prop="options">
      <template #default="scope">
        <div class="text-blue-500 cursor-pointer select-none">
          <a @click="onSelFunction(scope.row)" class="inline-block">Trigger</a>
          <el-divider direction="vertical" />
          <a @click="onShowDetail(scope.row)" class="inline-block">Detail</a>
          <el-divider direction="vertical" />
          <a
            @click="onStart(scope.row)"
            v-show="!scope.row.status"
            class="inline-block"
            >Start</a
          >
          <a
            @click="onStop(scope.row)"
            v-show="scope.row.status"
            class="inline-block"
            >Stop</a
          >
          <el-divider direction="vertical" />
          <a @click="onDelete(scope.row)" class="inline-block">Delete</a>
        </div>
      </template>
    </el-table-column>
    <template #empty>
      <el-empty :description="$t('comp.emptyData')"></el-empty>
    </template>
  </el-table>
</template>

<script>
import { deleteFunc, startFunc, stopFunc } from "@/api/func";

export default {
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
  },
  methods: {
    onDelete(func) {
      const { name = "" } = func;
      const _this = this;
      this.$confirm(_this.$t("func.deleteFuncNotice", [name]), "comp.noice", {
        type: "warning",
      })
        .then(async () => {
          try {
            await deleteFunc(name);
            _this.$store.dispatch("app/onRefresh");
            _this.$message.success(_this.$t("func.deleteSuccess", [name]));
          } catch (error) {
            this.$message.info(_this.$t("func.deleteFail", [name]));
          }
        })
        .catch(() => {
          this.$message.info(_this.$t("func.cancelDelete", [name]));
        });
    },
    onStart(text) {
      const { name = "" } = text;
      const _this = this;
      async function Start() {
        try {
          const res = await startFunc(name);
          text.status = true;
          _this.$message.success(_this.$t("func.startSuccess"));
        } catch (error) {
          _this.$message.error(_this.$t("func.startFail"));
          console.error(error);
        }
      }
      Start();
    },
    onStop(text) {
      const { name = "" } = text;
      const _this = this;
      async function Stop() {
        try {
          await stopFunc(name);
          text.status = false;
          _this.$message.success(_this.$t("func.stopSuccess"));
        } catch (error) {
          _this.$message.error(_this.$t("func.stopFail"));
          console.error(error);
        }
      }
      Stop();
    },
  },
};
</script>