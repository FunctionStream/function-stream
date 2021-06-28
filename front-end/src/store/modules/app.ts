const app = {
  namespaced: true,
  state: {
    sideCollapsed: false,
    isRefresh: false // 与 @/layouts/BaseLayout.vue 配合刷新
  },
  actions: {
    onChangeSideCollapsed({ commit }, state: Boolean) {
      commit('save', { sideCollapsed: state })
    },
    onRefresh({ commit }, state = true) {
      commit('save', { isRefresh: state })
    }
  },
  mutations: {
    save(state, payload) {
      if ({}.toString.call(payload) !== '[object Object]') {
        console.error('payload must be an object')
        return
      }
      for (const key in payload) {
        if (Object.prototype.hasOwnProperty.call(payload, key)) {
          const value = payload[key]
          state[key] = value
        }
      }
    }
  }
}

export default app
