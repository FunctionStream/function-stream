<template>
  <div class="main">
    <a-form class="user-layout-login" ref="formLogin" :form="form" @submit="handleSubmit">
      <a-alert
        v-if="isLoginError"
        type="error"
        showIcon
        style="margin-bottom: 24px;"
        :message="$t('user.login.message-invalid-credentials')" />
      <a-form-item>
        <a-input
          size="large"
          type="text"
          :placeholder="$t('user.login.username.placeholder')"
          v-decorator="[ 'username',{rules: validateRules.usernameRule , validateTrigger: 'change'}]">
          <a-icon slot="prefix" type="user" :style="{ color: 'rgba(0,0,0,.25)' }" />
        </a-input>
      </a-form-item>

      <a-form-item>
        <a-input-password
          size="large"
          :placeholder="$t('user.login.password.placeholder')"
          v-decorator="['password', {rules: validateRules.passwordRule, validateTrigger: 'blur'}]">
          <a-icon slot="prefix" type="lock" :style="{ color: 'rgba(0,0,0,.25)' }" />
        </a-input-password>
      </a-form-item>

      <a-form-item>
        <a-checkbox v-model="needRememberMe">{{ $t('user.login.remember-me') }}</a-checkbox>
      </a-form-item>

      <a-form-item style="margin-top:24px">
        <a-button
          size="large"
          type="primary"
          htmlType="submit"
          class="login-button"
          :loading="state.loginBtn"
          :disabled="state.loginBtn">{{ $t('user.login.login') }}
        </a-button>
      </a-form-item>

    </a-form>
  </div>
</template>

<script>
import md5 from 'md5'
import storage from 'store'
import { mapActions } from 'vuex'
import { timeFix } from '@/utils/util'
import { getSmsCaptcha } from '@/api/login'
import { LOGIN_INFO, REMEMBER_ME } from '@/store/mutation-types.js'

export default {
  data () {
    return {
      loginBtn: false,
      // login type: 0 email, 1 username, 2 telephone
      loginType: 0,
      isLoginError: false,
      form: this.$form.createForm(this),
      state: {
        time: 60,
        loginBtn: false,
        // login type: 0 email, 1 username, 2 telephone
        loginType: 0,
        smsSendBtn: false
      },
      needRememberMe: false,
      validateRules: {
        usernameRule: [{ required: true, message: this.$t('user.userName.required') }, { validator: this.handleUsernameOrEmail }],
        passwordRule: [{ required: true, message: this.$t('user.password.required') }],
        mobileRule: [{ required: true, message: this.$t('user.login.mobile.placeholder'), pattern: /^1[34578]\d{9}$/ }],
        verificationCodeRule: [{ required: true, message: this.$t('user.verification-code.required') }]
      }
    }
  },
  created () {
    const needRememberMe = storage.get(REMEMBER_ME)
    if (needRememberMe === 'true') {
      this.needRememberMe = true
    }
  },
  methods: {
    ...mapActions(['Login', 'Logout']),
    handleUsernameOrEmail (rule, value, callback) {
      const { state } = this
      const regex = /^([a-zA-Z0-9_-])+@([a-zA-Z0-9_-])+((\.[a-zA-Z0-9_-]{2,3}){1,2})$/
      if (regex.test(value)) {
        state.loginType = 0
      } else {
        state.loginType = 1
      }
      callback()
    },
    handleSubmit (e) {
      e.preventDefault()
      const { form: { validateFields }, state, Login } = this

      state.loginBtn = true

      validateFields(['username', 'password'], { force: true }, (err, values) => {
        if (!err) {
          const loginParams = { ...values, password: md5(values.password) }

          if (this.needRememberMe) {
            storage.set(REMEMBER_ME, 'true')
            storage.set(LOGIN_INFO, JSON.stringify(loginParams))
          } else {
            storage.set(REMEMBER_ME, 'false')
          }

          Login(loginParams)
            .then(res => this.loginSuccess(res))
            .catch(err => this.requestFailed(err))
            .finally(() => {
              state.loginBtn = false
            })
        } else {
          setTimeout(() => {
            state.loginBtn = false
          }, 600)
        }
      })
    },
    getCaptcha (e) {
      e.preventDefault()
      const { form: { validateFields }, state } = this

      validateFields(['mobile'], { force: true }, (err, values) => {
        if (!err) {
          state.smsSendBtn = true

          const interval = window.setInterval(() => {
            if (state.time-- <= 0) {
              state.time = 60
              state.smsSendBtn = false
              window.clearInterval(interval)
            }
          }, 1000)

          const hide = this.$message.loading('验证码发送中..', 0)
          getSmsCaptcha({ mobile: values.mobile }).then(res => {
            setTimeout(hide, 2500)
            this.$notification['success']({
              message: '提示',
              description: '验证码获取成功，您的验证码为：' + res.result.captcha,
              duration: 8
            })
          }).catch(err => {
            setTimeout(hide, 1)
            clearInterval(interval)
            state.time = 60
            state.smsSendBtn = false
            this.requestFailed(err)
          })
        }
      })
    },
    loginSuccess () {
      this.$router.push({ path: '/' })
      // Delayed by 1s to show the welcome pop
      setTimeout(() => {
        this.$notification.success({
          message: '欢迎',
          description: `${ timeFix() }，欢迎回来`
        })
      }, 1000)
      this.isLoginError = false
    },
    requestFailed (err) {
      this.isLoginError = true
      this.$notification['error']({
        message: '错误',
        description: ((err.response || {}).data || {}).message || '请求出现错误，请稍后再试',
        duration: 4
      })
    }
  }
}
</script>

<style lang="less" scoped>
.user-layout-login {
  label {
    font-size: 14px;
  }

  .getCaptcha {
    display: block;
    width: 100%;
    height: 40px;
  }

  .forge-password {
    font-size: 14px;
    float: right;
  }

  button.login-button {
    padding: 0 15px;
    font-size: 16px;
    height: 40px;
    width: 100%;
  }

  .user-login-other {
    text-align: left;
    margin-top: 24px;
    line-height: 22px;

    .item-icon {
      font-size: 24px;
      color: rgba(0, 0, 0, 0.2);
      margin-left: 16px;
      vertical-align: middle;
      cursor: pointer;
      transition: color 0.3s;

      &:hover {
        color: #1890ff;
      }
    }

    .register {
      float: right;
    }
  }
}
</style>
