<template>
  <div class="flow-wrapper">
    <div class="left">
      <div ref="container" class="container1"></div>
    </div>
    <div class="right">
      <div ref="app2Dom"><!--这是画布--></div>
    </div>
  </div>
  <ul v-show="showFSBoxMenu" class="menu">
    <!-- todo 菜单样式需要完善    -->
    <li class="delete" @click="deleteFuncItem">删除</li>
  </ul>
</template>

<script>
  import { onMounted, ref } from 'vue'
  import { Addon } from '@antv/x6/lib'
  import { FSBusiness } from '@/utils/fs-graph.js'
  import { fsBoxMenu, showFSBoxMenu } from '@/views/data-flow-diagram/store.js'
  const { FSBox, FSGraph } = FSBusiness
  const { Stencil } = Addon
  const MOCK_FUNC_LIST = ['func1', 'func2', 'func3']
  export default {
    setup() {
      const app2Dom = ref(null)
      const container = ref(null)
      const deleteFuncItem = () => {
        const { view } = fsBoxMenu.value
        view.cell.remove()
        // todo 删除有连接的线
        showFSBoxMenu.value = false
        fsBoxMenu.value = {}
      }
      onMounted(() => {
        const graph = new FSGraph(app2Dom.value)
        const stencil = new Stencil({
          title: 'function type',
          target: graph,
          search(cell, keyword) {
            return cell.shape.indexOf(keyword) !== -1
          },
          placeholder: '',
          stencilGraphWidth: 200,
          stencilGraphHeight: 180,
          groups: [{ name: 'function', title: 'function group1' }]
        })
        container.value.appendChild(stencil.container)
        const mockFuncNodeList = MOCK_FUNC_LIST.map((funcName) => new FSBox(funcName))
        stencil.load(mockFuncNodeList, 'function')
      })
      return {
        app2Dom,
        container,
        showFSBoxMenu,
        deleteFuncItem
      }
    }
  }
</script>

<style>
  .flow-wrapper {
    display: flex;
    height: 100%;
  }
  .flow-wrapper .left {
    display: flex;
    flex-flow: column;
    position: relative;
    flex: 0 200px;
    background: greenyellow;
  }
  .flow-wrapper .left .container1 {
    position: relative;
    height: 100%;
  }
  .flow-wrapper .right {
    flex: 1;
    position: relative;
  }
  ul.menu {
    position: fixed;
    top: 100px;
    left: 500px;
    width: 100px;
    background: pink;
    z-index: 999;
  }
  ul.menu li {
    cursor: pointer;
  }
</style>
