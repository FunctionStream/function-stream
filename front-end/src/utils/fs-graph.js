// x6业务模板: https://x6.antv.vision/zh/examples/gallery#category-%E6%A0%87%E7%AD%BE
//            https://x6.antv.vision/zh/examples/showcase/practices#validate-connection
// x6文档:     https://x6.antv.vision/zh
import { Graph, Shape } from '@antv/x6/lib'
import { fsBoxMenu, showFSBoxMenu } from '@/views/data-flow-diagram/store.js'

// 盒子样式 (todo 暂定 可能需要大改)
const SHAPE_CONFIG = {
  DEFAULT_FUNCTION_BOX_WIDTH: 80,
  DEFAULT_FUNCTION_BOX_HEIGHT: 50,
  DEFAULT_PORT_STYLE: {
    attrs: {
      circle: {
        r: 6,
        magnet: true,
        stroke: '#ffa940',
        fill: '#fff',
        strokeWidth: 2
      }
    }
  } // 连接点的样式
}

// 定义画布属性
const GRAPH_CONFIG = {
  DEFAULT_GRAPH_WIDTH: 800,
  DEFAULT_GRAPH_HEIGHT: 600,
  DEFAULT_MAGNET_AVAILABLE: {
    name: 'stroke',
    args: {
      attrs: {
        fill: '#fff',
        stroke: '#47C769'
      }
    }
  }, // 能连接的点
  DEFAULT_MAGNET_ADSORBED: {
    name: 'stroke',
    args: {
      attrs: {
        fill: '#fff',
        stroke: '#31d0c6'
      }
    }
  }, // 吸附样式
  DEFAULT_CONNECTING: {
    snap: true,
    allowBlank: false,
    allowLoop: false,
    highlight: true,
    connector: 'rounded', // 圆形
    connectionPoint: 'boundary', // 边界
    // router: {                // 这是线条的曲折
    //   name: 'er',
    //   args: {
    //     direction: 'V'
    //   }
    // },
    createEdge() {
      // todo 线条要换成圆曲线
      return new Shape.Edge({
        attrs: {
          line: {
            strokeWidth: 1,
            targetMarker: {
              name: 'classic',
              size: 7
            }
          }
        }
      })
    },
    validateConnection({ targetView, targetMagnet }) {
      // 过滤②右边的节点
      if (!targetMagnet) return false
      if (targetMagnet.getAttribute('port-group') === 'rightPort') return false
      // todo 这里要跟业务进行定义 (原来是过滤重复的线条)
      // if (targetView) {
      //   const node = targetView.cell
      //   const portId = targetMagnet.getAttribute('port')
      //   const inComePorts = this.getIncomingEdges(node)?.map((x) => x.getTargetPortId()) || [] // 获取所有in的port
      //   const isEdgeDuplicated = inComePorts.find((x) => x === portId)
      //   if (isEdgeDuplicated) return false
      // }

      return true
    }
  } // 连接的线条样式
}

/***********中间层开始 这是对x6的第一层包装****************/
class FSMiddleRect extends Shape.Rect {}

FSMiddleRect.config({
  shape: 'rect',
  width: SHAPE_CONFIG.DEFAULT_FUNCTION_BOX_WIDTH,
  height: SHAPE_CONFIG.DEFAULT_FUNCTION_BOX_HEIGHT,
  attrs: {
    rect: { fill: '#f5f5f5', stroke: '#d9d9d9', strokeWidth: 2 },
    text: { text: 'rect' }
  },
  ports: {
    groups: {
      topPort: { position: 'top', label: { position: 'top' }, ...SHAPE_CONFIG.DEFAULT_PORT_STYLE },
      rightPort: { position: 'right', label: { position: 'right' }, ...SHAPE_CONFIG.DEFAULT_PORT_STYLE },
      bottomPort: { position: 'bottom', label: { position: 'bottom' }, ...SHAPE_CONFIG.DEFAULT_PORT_STYLE },
      leftPort: { position: 'left', label: { position: 'left' }, ...SHAPE_CONFIG.DEFAULT_PORT_STYLE }
    },
    items: [
      // todo 这里的文字要用户自己双击加上
      // { id: 'top_port1', group: 'topPort', attrs: { text: { text: 'top_port1' } } },
      { id: 'right_port1', group: 'rightPort', attrs: { text: { text: 'right_port1' } } },
      // { id: 'bottom_port1', group: 'bottomPort', attrs: { text: { text: 'bottom_port1' } } },
      { id: 'left_port1', group: 'leftPort', attrs: { text: { text: 'left_port1' } } }
    ]
  }
})

/***********中间层结束****************/

/***********业务层开始 这是对x6的第二层包装,就是function stream自己的业务****************/
class FSBox {
  constructor(funcName, x6CellConfig) {
    const attrs = {
      attrs: { text: { text: funcName } },
      ...x6CellConfig
    }
    return new FSMiddleRect(attrs)
  }
}

class FSGraph {
  constructor(containerDom, ...x6GraphConfig) {
    const attrs = this.getInitGraphConfig(containerDom, ...x6GraphConfig)

    this._graph = new Graph(attrs)
    this._registerEvent()

    return this._graph
  }

  getInitGraphConfig(containerDom, ...x6GraphConfig) {
    if (x6GraphConfig.find((x) => typeof x !== 'object')) {
      throw new Error('检查参数, 参数应该是对象才对')
    }
    const extendObj = Object.assign(...x6GraphConfig, {}) // 解构参数并重新组装
    const config = {
      container: containerDom,
      grid: true,
      width: GRAPH_CONFIG.DEFAULT_GRAPH_WIDTH,
      height: GRAPH_CONFIG.DEFAULT_GRAPH_HEIGHT,
      highlighting: {
        magnetAvailable: GRAPH_CONFIG.DEFAULT_MAGNET_AVAILABLE,
        magnetAdsorbed: GRAPH_CONFIG.DEFAULT_MAGNET_ADSORBED
      },
      connecting: GRAPH_CONFIG.DEFAULT_CONNECTING,
      ...extendObj
    }
    return config
  }

  _registerEvent() {
    // this.registerEdgeConnectedEvent()
    this.registerRightClickEvent()
    this.registerDbclickEvent()
  }

  registerEdgeConnectedEvent() {
    // 这个可能用不到
    // this._graph.on('edge:connected', ({ previousView, currentView }) => {
    //   console.log(previousView, currentView)
    //   // todo 取消高亮
    //   const shape = currentView.cell
    //   // const allIncomePorts = this._graph.getIncomingEdges(shape)
    //   const rightPorts = shape.getPortsByGroup('rightPort')
    //   const topPorts = shape.getPortsByGroup('topPort')
    //   const bottomPorts = shape.getPortsByGroup('bottomPort')
    //   const allIncomePorts = [...rightPorts, ...topPorts, ...bottomPorts]
    //
    // })
  }

  registerRightClickEvent() {
    this._graph.on('cell:contextmenu', ({ e, x, y, cell, view }) => {
      showFSBoxMenu.value = true
      fsBoxMenu.value = { e, x, y, cell, view }
    })
  }

  registerDbclickEvent() {
    this._graph.on('cell:dblclick', ({ e, x, y, cell, view }) => {
      const { target } = e
      const parentElement = target.parentElement
      if ([...parentElement.classList].includes('x6-port-label')) {
        // todo 双击节点
        console.log(target.textContent)
      }
    })
  }
}

/***********业务层结束****************/

export const FSMiddleLayer = {
  FSMiddleRect
}

export const FSBusiness = {
  FSBox,
  FSGraph
}
