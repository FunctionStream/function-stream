<template>
  <div class="account-settings-info-view">
    <a-form :label-col="{ span: 3 }"
            :wrapper-col="{ span: 3, offset: 12 }">
      <a-form-item :label="$t('account.settings.common.remember-me')">
        <a-switch checked-children="开"
                  un-checked-children="关"
                  v-model="rememberMe"
                  @change="handleRememberMe"></a-switch>
      </a-form-item>
    </a-form>

  </div>
</template>

<script>
import { baseMixin } from '@/store/app-mixin'
import { REMEMBER_ME } from '@/store/mutation-types.js'
import * as storage from 'store'

export default {
  mixins: [baseMixin],
  components: {},
  data () {
    return {
      rememberMe: storage.get(REMEMBER_ME) === 'true'
    }
  },
  methods: {
    handleRememberMe (needRememberMe) {
      storage.set(REMEMBER_ME, String(needRememberMe))
    }
  }
}
</script>

<style lang="less" scoped>

  .account-settings-info-view {
    max-width: 900px;
  }

</style>
