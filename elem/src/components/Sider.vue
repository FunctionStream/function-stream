<template>
  <component
    :is="sideComp"
    :model-value="openSide"
    :show-close="false"
    direction="ltr"
    destroy-on-close
    :with-header="false"
    :before-close="onChangMenuCollapse"
    size="256"
  >
    <el-menu
      :default-active="$route.path"
      class="el-menu-vertical"
      :collapse="!openSide"
      :router="true"
    >
      <div class="text-2xl font-bold flex justify-center p-2 py-4">
        <slot>
          <span class="truncate">{{ title }}</span>
        </slot>
      </div>
      <Menu :menus="menus" />
    </el-menu>
  </component>
</template>

<script>
import Menu from "./Menu.vue";
import { computed, ref } from "vue";
import { useStore } from "vuex";

export default {
  setup() {
    const store = useStore();
    const sideComp = ref("div");

    return {
      openSide: computed(() => !store.state.app.sideCollapsed),
      onChangMenuCollapse: () =>
        store.dispatch(
          "app/onChangeSideCollapsed",
          !store.state.app.sideCollapsed
        ),
      sideComp,
    };
  },
  data() {
    return {
      screenWidth: document.body.clientWidth,
    };
  },
  props: {
    title: {
      type: String,
      default: "Project",
    },
    menus: {
      type: Array,
      default: [],
    },
  },
  components: {
    Menu,
  },
  mounted() {
    const _this = this;
    this.onChangeDrawer();
    window.onresize = () => {
      _this.screenWidth = document.body.clientWidth;
    };
  },
  watch: {
    screenWidth() {
      const _this = this;
      if (!this.timer) {
        this.timer = true;
        setTimeout(function () {
          _this.onChangeDrawer();
          _this.timer = false;
        }, 500);
      }
    },
  },
  methods: {
    onChangeDrawer() {
      if (document.body.clientWidth < 600 && this.sideComp != "ElDrawer") {
        this.sideComp = "ElDrawer";
      }
      if (document.body.clientWidth > 600 && this.sideComp != "div") {
        this.sideComp = "div";
      }
    },
  },
};
</script>

<style>
.el-menu-vertical:not(.el-menu--collapse) {
  width: 256px;
  min-height: 100vh;
  padding-bottom: 64px;
}
.el-menu--collapse {
  min-height: 100vh;
}
</style>
