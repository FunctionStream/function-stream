// \todo 这些固定变量需要抽出去到setting.json或config.json里面
// 新增删除等按钮(todo 暂定 可能不需要这个)
import { Graph } from '@antv/x6/lib'

const DEFAULT_BUTTON_WIDTH = 70
const DEFAULT_BUTTON_HEIGHT = 40
const DEFAULT_BUTTON_X = 0
const DEFAULT_BUTTON_Y = 0

// function 的item (todo 暂定 可能需要大改)
const DEFAULT_FUNCTION_BOX_WIDTH = 60
const DEFAULT_FUNCTION_BOX_HEIGHT = 30
const DEFAULT_FUNCTION_BOX_X = 80
const DEFAULT_FUNCTION_BOX_Y = 80

// 定义画布属性
const DEFAULT_GRAPH_WIDTH = 800
const DEFAULT_GRAPH_HEIGHT = 600

// 桩的样式
const DEFAULT_PORT_STYLE = {
  attrs: {
    circle: {
      r: 6,
      magnet: true,
      stroke: '#31d0c6',
      strokeWidth: 2,
      fill: '#fff'
    }
  }
}


// todo 需要接入typescript进行规范化
// fixme id 是必须的, 生成后会返回
// todo 最终目标是做业务中间层, 在antd/x6上做增删改查等二次封装

// 矩形
/**
 * todo 这个还是有问题, 有其他参数需要扩展的时候应该自动增加
 */
export class Cube {
  constructor({ width, height, x, y, shape, attrs, data, html, ports }) {
    this.x = x
    this.y = y
    this.width = width
    this.height = height
    this.shape = shape
    this.attrs = attrs
    this.data = data
    this.ports = ports

    this.html = html
  }
}

export class Button extends Cube {
  constructor(
    _btnHTML = `请输入文本内容`,
    X6Parameters = {
      shape: 'html',
      x: DEFAULT_BUTTON_X,
      y: DEFAULT_BUTTON_Y,
      width: DEFAULT_BUTTON_WIDTH,
      height: DEFAULT_BUTTON_HEIGHT,
      attrs: {
        body: { width: 200, height: 200, stroke: 'red' }
      },
      html: {
        render: () => `${ _btnHTML }`
      } // todo 这里要有个默认功能
    }
  ) {
    const {
      shape = 'html',
      x = DEFAULT_BUTTON_X,
      y = DEFAULT_BUTTON_Y,
      width = DEFAULT_BUTTON_WIDTH,
      height = DEFAULT_BUTTON_HEIGHT,
      attrs = { body: { width: 200, height: 200, stroke: 'red' } },
      html = { render: () => `${ _btnHTML }` }
    } = X6Parameters
    super({ shape, x, y, width, height, attrs, html })
  }

  // todo 点击事件
  clickButton() {}
}

export class FunctionNode extends Cube {
  constructor(
    _functionContentHTML = `请输入方法名字`,
    X6Parameters = {
      shape: 'html',
      x: DEFAULT_FUNCTION_BOX_X,
      y: DEFAULT_FUNCTION_BOX_Y,
      width: DEFAULT_FUNCTION_BOX_WIDTH,
      height: DEFAULT_FUNCTION_BOX_HEIGHT,
      attrs: {
        body: { width: 200, height: 200, stroke: 'red' }
      },
      html: {
        render: () => `${ _functionContentHTML }`
      } // todo 这里要有个默认功能
    }
  ) {
    super({ ...X6Parameters })
  }
}

export class FunctionReact extends Cube {
  constructor(
    _functionContentHTML = `请输入方法名字`,
    X6Parameters = {
      shape: 'rect',
      x: DEFAULT_FUNCTION_BOX_X,
      y: DEFAULT_FUNCTION_BOX_Y,
      width: DEFAULT_FUNCTION_BOX_WIDTH,
      height: DEFAULT_FUNCTION_BOX_HEIGHT,
      attrs: {
        rect: { fill: '#31D0C6', stroke: '#4B4A67', strokeWidth: 6 },
        text: { text: 'rect', fill: 'white' }
      },
      ports: [
        { id: 'port1', ...DEFAULT_PORT_STYLE },
        { id: 'port2', ...DEFAULT_PORT_STYLE }
      ],
      html: {
        render: () => `${ _functionContentHTML }`
      } // todo 这里要有个默认功能
    }) {

    super({ ...X6Parameters })
  }
}

export class FSGraph {
  constructor(containerDom, x6GraphConfig) {
    const { width = DEFAULT_GRAPH_WIDTH, height = DEFAULT_GRAPH_HEIGHT, grid = true } = x6GraphConfig
    const container = containerDom

    return new Graph({ container, width, height, grid })
  }
}
