<template>
    <div :class="{'has-logo':showLogo}" :style="{ backgroundColor: settings.sideTheme === 'theme-dark' ? variables.menuBackground : variables.menuLightBackground }">
        <!-- <logo v-if="showLogo" :collapse="isCollapse" /> -->
        <div class="hamburger-area">
            <div class="hamburger-title" v-if="sidebar.opened">Iceberg湖仓管理工具</div>
            <hamburger id="hamburger-container" :is-active="sidebar.opened" class="hamburger-container" @toggleClick="toggleSideBar" />
        </div>
        <el-scrollbar :class="settings.sideTheme" wrap-class="scrollbar-wrapper">
            <el-menu
                :default-active="activeMenu"
                :collapse="isCollapse"
                :background-color="settings.sideTheme === 'theme-dark' ? variables.menuBackground : variables.menuLightBackground"
                :text-color="settings.sideTheme === 'theme-dark' ? variables.menuColor : variables.menuLightColor"
                :unique-opened="true"
                :active-text-color="settings.theme"
                :collapse-transition="false"
                mode="vertical"
            >
                <sidebar-item
                    v-for="(route, index) in sidebarRouters"
                    :key="route.path  + index"
                    :item="route"
                    :base-path="route.path"
                />
            </el-menu>
        </el-scrollbar>
    </div>
</template>

<script>
import { mapGetters, mapState } from "vuex";
import Logo from "./Logo";
import SidebarItem from "./SidebarItem";
import variables from "@/assets/styles/variables.scss";
import Hamburger from '@/components/Hamburger'

export default {
    components: { SidebarItem, Logo, Hamburger },
    computed: {
        ...mapState(["settings"]),
        ...mapGetters(["sidebarRouters", "sidebar"]),
        activeMenu() {
            const route = this.$route;
            const { meta, path } = route;
            // if set path, the sidebar will highlight the path you set
            if (meta.activeMenu) {
                return meta.activeMenu;
            }
            return path;
        },
        showLogo() {
            return this.$store.state.settings.sidebarLogo;
        },
        variables() {
            return variables;
        },
        isCollapse() {
            return !this.sidebar.opened;
        }
    },
    methods: {
        toggleSideBar() {
        this.$store.dispatch('app/toggleSideBar')
        },
    }
};
</script>
<style lang="scss">
.hamburger-area {
    display: flex;
    align-items: center;
    justify-content: space-between;
    line-height: 54px;
    height: 54px;
    font-size: 16px;
    color: #24324c;
    background-color: #fff;
    padding: 0 20px;
    border-bottom: 1px solid #f0f1f3;
    .hamburger-title {
        white-space: nowrap;
    }
    .hamburger-container {
        padding: 0 !important;
        cursor: pointer;
        -webkit-tap-highlight-color: transparent;
    }
}

</style>
