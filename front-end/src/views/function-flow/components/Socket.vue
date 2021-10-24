<template>
  <div class="socket" :class="kebab([type, socket.name])" :title="socket.name"></div>
</template>

<script>
  export default {
    props: ['type', 'socket']
  }
</script>

<style>
  .socket {
    display: inline-block;
    cursor: pointer;
    border: 1px solid white;
    border-radius: 12px;
    width: 24px;
    height: 24px;
    margin: 6px;
    vertical-align: middle;
    background: #96b38a;
    z-index: 2;
    box-sizing: border-box;
  }

  .socket:hover {
    border-width: 4px;
  }

  .socket.multiple {
    border-color: yellow;
  }

  .socket.output {
    margin-right: -12px;
  }

  .socket.input {
    margin-left: -12px;
  }
</style>
