<template>
  <div class="flow-wrapper">
    <div class="left">
      <div ref="container" class="container1"></div>
    </div>
    <div class="right">
      <div ref="app2Dom"></div>
    </div>
  </div>
</template>

<script>
  import { onMounted, ref } from 'vue'
  import { Addon } from '@antv/x6/lib'
  import { FSGraph, FunctionReact } from '@/utils/fs-graph.js'

  const { Stencil } = Addon
  const MOCK_FUNC_LIST = ['func1', 'func2', 'func3']

  export default {
    setup() {
      const app2Dom = ref(null)
      const container = ref(null)
      const outBtnClick = () => {
        console.log('outBtnClick')
      }

      // fixme priority 1 连接节点, 右键删除连接线
      // fixme 方块拖到右边的画布以后再拖到左边会被盖住, 应该删掉或者修改index, 改成碰到边3了就拖不过去
      // fixme 添加删除功能
      // fixme 添加回退功能,(删错的情况下), 与ctrl+z, ctrl+shift+z绑定
      onMounted(() => {
        const graph = new FSGraph(app2Dom.value, {})

        const stencil = new Stencil({
          title: 'component',
          target: graph,
          search(cell, keyword) {
            return cell.shape.indexOf(keyword) !== -1
          },
          placeholder: '',
          collapsable: true,
          stencilGraphWidth: 200,
          stencilGraphHeight: 180,
          groups: [
            { name: 'function', title: 'Group(Collapsable)' }
          ]
        })

        container.value.appendChild(stencil.container)

        const mockFuncNodeList = MOCK_FUNC_LIST.map(funcName => new FunctionReact(funcName) )

        stencil.load(mockFuncNodeList, 'function')
      })

      return {
        app2Dom,
        container,
        outBtnClick
      }
    }
  }
</script>

<style>
  .flow-wrapper {
    display: flex;
    height: 100%;
    background: orange;
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
    background: pink;
  }
</style>
