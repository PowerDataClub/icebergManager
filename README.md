## 一、平台简介
* 本人学习hdfs、iceberg创建的项目
* icebergManager是一款基于hdfs存储，iceberg提供schema，使用spark或flink对结构化数据进行管理操作的工具，目前状态仅供于datapower成员学习hdfs、iceberg原生API使用，可以作为iceberg管理客户端简单使用。
![](.\pic\首页.png)
## 二、底层技术点
* 1、服务后台框架：ruoyi框架
* 2、大数据技术栈：hdfs、iceberg、spark、flink

## 三、功能说明
### 1、catalog目录管理
* iceberg catalog定义管理
* hdfs 连接信息定义管理
![](.\pic\目录.png)
### 2、结构化数据
* 数据入湖：支持hive、hdfs、kafka、mysql数据入iceberg
![](.\pic\入湖.png)
* 库表管理：iceberg目录、库、表树结构展示+库表元数据管理和操作
![](.\pic\库表.png)
* sql客户端：支持使用基于spark和flink本地客户端，通过sql对iceberg表进行数据操作（不推荐直接操作线上数据，目前spark支持yarn上执行，flink执行目前支持local）
![](.\pic\sql.png)
### 3、非结构化数据
* hdfs操作支持web管理（支持文件夹的web端上传、下载，不建议大批量数据的操作，web端目前没有通过linux服务本地中转下载）
![](.\pic\hdfs.png)

## 四、本地idea执行

### 0、环境工具要求

```
java8
nodejs 16+
```

### 1、初始化系统元数据
``` text
.\sql\icebergManager.sql
```
### 2、修改配置文件连接
``` text
.\iceberg-admin\src\main\resources\application.yml
.\iceberg-admin\src\main\resources\application-druid.yml
```
### 3、启动后台
``` text
.\iceberg-admin\src\main\java\com\powerdata\IcebergManagerApplication.java
```
### 4、启动前台
``` shell
cd .\iceberg-ui
npm install
npm run dev
```
## 五、关于我们
### 1、PowerData社区
![](.\pic\powerdata.png)
### 2、作者求感谢
wx：ToolsOnKeys
![](.\pic\actor.png)