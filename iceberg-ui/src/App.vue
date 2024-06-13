<template>
  <div id="app">
    <router-view />
    <theme-picker />
  </div>
</template>

<script>
import ThemePicker from "@/components/ThemePicker";

export default {
  name: "App",
  components: { ThemePicker },
    metaInfo() {
        return {
            title: this.$store.state.settings.dynamicTitle && this.$store.state.settings.title,
            titleTemplate: title => {
                return title ? `${title} - ${process.env.VUE_APP_TITLE}` : process.env.VUE_APP_TITLE
            }
        }
    }
};
</script>
<style scoped>
#app .theme-picker {
  display: none;
}
</style>
<style lang="scss">
.app-main {
  background: #f9f9f9;
}
// 左侧菜单栏样式
.el-menu-item, .el-submenu__title {
  height: 40px!important;
  line-height: 40px!important;
}
.el-dialog__body {
  padding: 20px;
}
/* scrollbar */
::-webkit-scrollbar {
    width: 10px;
    height: 8px;
    background-color: #f0f0f0;
}
/*定义滚动条轨道 内阴影+圆角*/
::-webkit-scrollbar-track {
    -webkit-box-shadow: inset 0 0 2px rgba(0,0,0,0.3);
    border-radius: 10px;
    background-color: #f0f0f0;
}
/*定义滑块 内阴影+圆角*/
::-webkit-scrollbar-thumb {
    border-radius: 10px;
    -webkit-box-shadow: inset 0 0 1px #4383b5;
    background-color: #999;
    /* background: linear-gradient(to bottom, #15619b, #3682bc, #15619b); */
    /* background: linear-gradient(to bottom, #232939, #3b3e44, #232939); */
    /* border:1px solid #257d9a; */
}
::-webkit-scrollbar-thumb:horizontal {
    border-radius: 10px;
    -webkit-box-shadow: inset 0 0 1px #4383b5;
    background-color: #999;
    /* background: linear-gradient(to right, #15619b, #3682bc, #15619b); */
    /* background: linear-gradient(to bottom, #232939, #3b3e44, #232939); */
    /* border:1px solid #257d9a; */
}
</style>
