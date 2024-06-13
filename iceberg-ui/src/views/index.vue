<template>
  <div class="app-container home">
    <div class="home-header">
      <div class="home-header-title">Iceberg湖仓管理工具@POWERDATA</div>
      <div class="home-header-title">数据湖存储依赖hdfs，通过iceberg构建schema，计算支持原生api、spark、flink构建的iceberg湖仓管理工具。目前该工具仅限于学习hdfs、iceberg相关API使用，不建议使用在生产环境</div>
    </div>
    <div class="home-center">功能说明</div>
    <div class="home-footer">
      <div class="home-footer-item" v-for="(item,index) in footerList" :key="index">
        <img class="home-footer-item-img" :src="item.img" alt="">
        <div class="home-footer-item-title">{{item.title}}</div>
        <div class="home-footer-item-content">{{item.content}}</div>
      </div>
    </div>
  </div>
</template>

<script>
export default {
  name: "Index",
  data() {
    return {
      footerList:[
        {
          img:require('../assets/indexImgs/catalog.jpeg'),
          title:'catalog管理',
          content:'1、支持iceberg两种catalog模式（hive+hadoop）\n' +
            '2、虚拟的catalog模式（file对应的hdfs连接配置）'
        },
        {
          img:require('../assets/indexImgs/iceberg.jpeg'),
          title:'iceberg管理',
          content:'1、支持结构化数据入湖（自定义数据集成方式，不推荐）\n' +
            '2、iceberg库表管理（基于原生iceberg API获取）\n' +
            '3、sql执行客户端（支持spark和flink两种计算引擎）\n'+
            '4、spark支持本地和yarn上两种运行模式，flink目前只支持local运行模式'
        },
        {
          img:require('../assets/indexImgs/hdfs.jpg'),
          title:'hdfs管理',
          content:'支持hdfs文件系统界面化操作（可以学习hdfs方向接口）'
        },
        {
          img:require('../assets/indexImgs/powerdata.png'),
          title:'联系我们',
          content:'我们是由一群数据从业人员，因为热爱凝聚在一起，以开源精神为基础，组成的PowerData数据之力社区。'
        },
      ],
    };
  },
  methods: {

  }
};
</script>

<style scoped lang="scss">
.home {
  .home-header {
    margin: -15px;
    height: 280px;
    background: url('../assets/indexImgs/headerBg.png') no-repeat;
    background-size: 100% 100%;
    display: flex;
    flex-direction: column;
    justify-content: center;
    .home-header-title {
      margin-left: 75px;
      margin-bottom: 20px;
      font-family: PingFangSC-Semibold;
      font-size: 36px;
      font-weight: normal;
      color: #00102f;
    }
    .home-header-content {
      margin-left: 75px;
      width: 870px;
      font-family: PingFangSC-Regular;
      font-size: 16px;
      font-weight: normal;
      color: #00102f;
    }
  }
  .home-center{
    width: 100%;
    text-align: center;
    margin-top: 50px;
    margin-bottom: 40px;
    font-family: PingFangSC-Semibold;
    font-size: 24px;
    font-weight: normal;
    color: #00102f;
  }
  .home-footer {
    display: flex;
    align-items: center;
    .home-footer-item {
      min-height: 420px;
      flex: auto;
      display: flex;
      flex-direction: column;
      align-items: center;
      justify-content: flex-start;
      border: solid 1px #dee3ef;
      margin: 0 5px;
      .home-footer-item-img {
        width: 154px;
	      height: 160px;
        margin-top: 28px;
      }
      .home-footer-item-title {
        font-family: PingFangSC-Semibold;
        font-size: 24px;
        font-weight: normal;
        color: #00102f;
        margin: 40px 0;
      }
      .home-footer-item-content {
        max-width: 350px;
        font-family: PingFangSC-Regular;
        font-size: 14px;
        font-weight: normal;
        color: rgba(0, 16, 47, 0.65);
        margin: 0 25px;
      }
    }
  }
}
</style>

