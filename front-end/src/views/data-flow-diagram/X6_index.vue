<template>
  <div class="left"></div>
  <div class="right"></div>

  <el-button>新增</el-button>
  <el-button>删除</el-button>
  <el-button>拖动</el-button>
  <el-button>Default</el-button>
  <div ref="app2Dom"></div>
</template>

<script>
  import { onMounted, ref } from 'vue'
  import { Graph } from '@antv/x6/lib'
  import { Button, FunctionBoxNode } from '@/utils/fs-graph.js'

  export default {
    setup() {
      const app2Dom = ref(null)
      const outBtnClick = () => {
        console.log('outBtnClick')
      }

      onMounted(() => {
        const graph = new Graph({
          container: app2Dom.value,
          width: 800,
          height: 600,
          grid: true
        })

        const newHtmlNode = new Button(`新增`)
        graph.addNode(newHtmlNode)

        const functionBoxNode = new FunctionBoxNode()
        const htmlNode = {
          ...functionBoxNode,
          data: {
            time: new Date().toString()
          },
          html: {
            render(node) {
              const data = node.getData()
              return `<div>
                    <span>${data.time}</span>
                  </div>`
            },
            shouldComponentUpdate(node) {
              // 控制节点重新渲染
              return node.hasChanged('data')
            }
          }
        }
        graph.addNode(htmlNode)
      })

      return {
        app2Dom,
        outBtnClick
      }
    }
  }
</script>

<style></style>
