const getType = (obj) => Object.prototype.toString.call(obj)

const isObject = (target) => (typeof target === 'object' || typeof target === 'function') && target !== null

const canTraverse = {
  '[object Map]': true,
  '[object Set]': true,
  '[object Array]': true,
  '[object Object]': true,
  '[object Arguments]': true
}
const mapTag = '[object Map]'
const setTag = '[object Set]'
const boolTag = '[object Boolean]'
const numberTag = '[object Number]'
const stringTag = '[object String]'
const symbolTag = '[object Symbol]'
const dateTag = '[object Date]'
const errorTag = '[object Error]'
const regexpTag = '[object RegExp]'
const funcTag = '[object Function]'

const handleRegExp = (target) => {
  const { source, flags } = target
  return new target.constructor(source, flags)
}

const handleFunc = (func) => {
  // 箭头函数直接返回自身
  if (!func.prototype) return func
  const bodyReg = /(?<={)(.|\n)+(?=})/m
  const paramReg = /(?<=\().+(?=\)\s+{)/
  const funcString = func.toString()
  // 分别匹配 函数参数 和 函数体
  const param = paramReg.exec(funcString)
  const body = bodyReg.exec(funcString)
  if(!body) return null
  if (param) {
    const paramArr = param[0].split(',')
    return new Function(...paramArr, body[0])
  } else {
    return new Function(body[0])
  }
}

const handleNotTraverse = (target, tag) => {
  const Ctor = target.constructor
  switch (tag) {
    case boolTag:
      return new Object(Boolean.prototype.valueOf.call(target))
    case numberTag:
      return new Object(Number.prototype.valueOf.call(target))
    case stringTag:
      return new Object(String.prototype.valueOf.call(target))
    case symbolTag:
      return new Object(Symbol.prototype.valueOf.call(target))
    case errorTag:
    case dateTag:
      return new Ctor(target)
    case regexpTag:
      return handleRegExp(target)
    case funcTag:
      return handleFunc(target)
    default:
      return new Ctor(target)
  }
}

export default function deepClone(target, map = new WeakMap()) {
  if (!isObject(target)) {
    return target
  }
  const type = getType(target)
  let cloneTarget
  if (!canTraverse[type]) {
    // 处理不能遍历的对象
    return handleNotTraverse(target, type)
  } else {
    // 这波操作相当关键，可以保证对象的原型不丢失！
    const ctor = target.constructor
    cloneTarget = new ctor()
  }

  if (map.get(target)) {
    return target
  }
  map.set(target, true)

  if (type === mapTag) {
    //处理Map
    target.forEach((item, key) => {
      cloneTarget.set(deepClone(key, map), deepClone(item, map))
    })
  }
  
  if (type === setTag) {
    //处理Set
    target.forEach((item) => {
      cloneTarget.add(deepClone(item, map))
    })
  }

  // 处理数组和对象
  for (const prop in target) {
    if (Object.prototype.hasOwnProperty.call(target, prop)) {
      cloneTarget[prop] = deepClone(target[prop], map)
    }
  }
  return cloneTarget
}
