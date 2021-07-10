const DEFAULT_BUTTON_WIDTH = 70
const DEFAULT_BUTTON_HEIGHT = 40
const DEFAULT_BUTTON_X = 0
const DEFAULT_BUTTON_Y = 0

const DEFAULT_FUNCTION_BOX_WIDTH = 160
const DEFAULT_FUNCTION_BOX_HEIGHT = 160
const DEFAULT_FUNCTION_BOX_X = 80
const DEFAULT_FUNCTION_BOX_Y = 80

// todo declare type shape = 'react' | 'html'
// todo id 是必须的, 生成后会返回
// todo 最终目标是做业务中间层, 在antd/x6上做增删改查等二次封装

// 矩形
export class Cube {
  constructor({ width, height, x, y, shape, attrs, data, html }) {
    this.x = x
    this.y = y
    this.width = width
    this.height = height
    this.shape = shape
    this.attrs = attrs
    this.data = data
    // if (typeof htmlContent === 'object') {
    //   // const _html = render ? { ...html, render } : html
    //   this.html = htmlContent
    // }
    // if (typeof htmlContent === 'string') {
    //   const render = `<span>${ htmlContent }</span>`
    //   this.html = htmlContent
    // }

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
        render: () => `${_btnHTML}`
      } // todo 这里要有个默认功能
    }
  ) {
    debugger
    const {
      shape = 'html',
      x = DEFAULT_BUTTON_X,
      y = DEFAULT_BUTTON_Y,
      width = DEFAULT_BUTTON_WIDTH,
      height = DEFAULT_BUTTON_HEIGHT,
      attrs = { body: { width: 200, height: 200, stroke: 'red' } },
      html = { render: () => `${_btnHTML}` }
    } = X6Parameters
    super({ shape, x, y, width, height, attrs, html })
  }

  // todo 点击事件
  clickButton() {}
}

export class FunctionBoxNode extends Cube {
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
        render: () => `${_functionContentHTML}`
      } // todo 这里要有个默认功能
    }
  ) {
    super({ ...X6Parameters })
  }
}
