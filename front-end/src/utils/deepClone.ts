const isObject = (target) => {
  return (typeof target === 'object' || typeof target === 'function') && target !== null
}

const deepClone = (target, map = new WeakMap()) => {
  if (map.get(target)) {
    return target
  }

  if (isObject(target)) { 
    map.set(target, true); 
    const cloneTarget = Array.isArray(target) ? []: {}; 
    for (let prop in target) { 
      if (target.hasOwnProperty(prop)) { 
          cloneTarget[prop] = deepClone(target[prop],map); 
      } 
    } 
    return cloneTarget; 
  } else { 
    return target; 
  } 
}