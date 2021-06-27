<template>
  <PageHeaderWrapper>
    <template #extra>
      <el-button icon="el-icon-circle-plus-outline" type="primary" class="mr-4">
        {{ $t("func.addFunc") }}
      </el-button>
    </template>
    <Func :data="functionList" v-loading="loading" />
  </PageHeaderWrapper>
</template>
<script>
import Func from "./components/Func.vue";
import { getList, getStatus } from "@/api/func";
export default {
  data() {
    return {
      functionList: [],
      loading: false,
    };
  },
  components: {
    Func,
  },
  async created() {
    try {
      this.loading = true;
      const res = await getList();
      if (Array.isArray(res)) {
        this.functionList = res?.map((name) => ({ key: name, name }));
        // get status
        res?.map(async (name, i) => {
          const res = await getStatus(name);
          this.functionList[i]["status"] = !!res?.numRunning;
          this.functionList[i]["statusInfo"] = res;
        });
      }
    } catch (e) {}
    this.loading = false;
  },
};
</script>