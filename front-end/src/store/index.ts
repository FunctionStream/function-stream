import { createStore } from 'vuex'
import app from './modules/app'

const store = createStore({
  modules: {
    app
  }
})

export default store
