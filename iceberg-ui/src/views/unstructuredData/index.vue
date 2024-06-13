<template>
  <div class="app-container">
    <div class="head-container header-area">
      <el-form :model="queryForm" size="small">
        <el-col :span="20" style="position: relative;">
          <el-input size="small" v-model="queryForm.dirPath" placeholder="请输入路径" @blur="showPathBtn = true,handleQuery" @focus="showPathBtn = false">
          <el-select
            style="width:160px"
            slot="prepend"
            size="small"
            v-model="currentCatalog"
            value-key="id"
            @change="()=>{queryForm.dirPath='/';handleQuery()}">
            <el-option v-for="(item,index) in catalogData" :key="'c'+index" :label="item.id" :value="item">
              <div>
                <el-tag size="small" :type="item.types=='hadoop'?'success':''">{{ item.types}}</el-tag>
                <span style="margin-left:10px">{{item.id}}</span>
                <span style="margin-left:10px">({{item.descs?item.descs:item.id}})</span>
              </div>
            </el-option>
          </el-select>
          <el-button slot="append" icon="el-icon-back" size="small" title="返回上一级" @click="handleBack"></el-button>
        </el-input>
        <div v-if="showPathBtn"
          style="position: absolute;
          top: 2px;
          left: 168px;
          background: #fff;">
          <el-button size="mini" @click="queryForm.dirPath='/';handleQuery()">/</el-button>
        </div>
        <div v-if="queryForm.dirPath!='/' && showPathBtn"
          style="position: absolute;
          top: 2px;
          left: 203px;
          background: #fff;
          max-width: calc(100% - 285px);
          white-space: nowrap;
          overflow-x: auto;">
          <span v-for="(item,index) in queryForm.dirPath.slice(1).split('/')" :key="index" style=''>
            <i v-if="index<queryForm.dirPath.slice(1).split('/').length" class="el-icon-caret-right" style="color:#999;font-size:12px;padding:0 2px"></i>
            <el-button size="mini" @click="handlePathBtnClick(index)">{{item}}</el-button>
          </span>
        </div>
        </el-col>
        <el-col :span="4">
        <el-form-item style="width: 100%;text-align: right;">
          <div style="display:flex;">
            <el-input placeholder="请输入文件名" v-model="queryForm.fileName" style="margin: 0 10px">
              <el-button slot="append" icon="el-icon-search" title="搜索" @click="handleQuery"></el-button>
            </el-input>
            <el-button icon="el-icon-refresh" size="small" title="重置" @click="resetQuery"></el-button>
            <!-- <el-upload
              style="margin-left:10px"
              action=""
              :multiple="true"
              :show-file-list="false"
              :http-request="handleUpload"
              :on-change="handleChange"
              :file-list="fileList"
              name="file">
              <el-button :loading="isImport" type="primary" icon="el-icon-upload2">上传</el-button>
            </el-upload> -->
            <el-button type="primary" icon="el-icon-upload2" size="small" style="position: relative;display:none" :key="'file'+isChange">
              <input type='file' multiple name='import' id="uploadFile" @change="handleUpload('uploadFile')"/>文件上传
            </el-button>
            <el-button type="primary" icon="el-icon-upload2" size="small" style="position: relative;display:none" :key="'dir'+isChange">
              <input type='file' multiple name='import' id="uploadDir" @change="handleUpload('uploadDir')" webkitdirectory/>文件夹上传
            </el-button>
          </div>
        </el-form-item>
        </el-col>
      </el-form>
      <el-progress v-if="uploadPercen && uploadPercen!=100" :percentage="uploadPercen"></el-progress>
    </div>
    <div @contextmenu.prevent="handleRightClickTable">
      <el-table
        border
        ref="fileTable"
        v-loading="loading"
        :data="fileData"
        @row-click="handleRowClick"
        :row-class-name="tableRowClassName"
        @row-dblclick="handleRowDbClick"
        @row-contextmenu="handleRightClick"
        height="calc(100vh - 272px)">
        <el-table-column label="名称" prop="fileName" :show-overflow-tooltip="true">
          <template slot-scope="scope">
            <div class="fileNameArea">
              <!-- <i :class="!scope.row.isFile?'el-icon-folder':'el-icon-document'"></i> -->
              <img v-if="!scope.row.isFile" class="fileIcon" style="width:20px;margin-right:4px" src="../../assets/images/dir.png" alt="" srcset="">
              <img v-else-if="scope.row.type == 'mp4'" class="fileIcon" src="../../assets/images/mp4.png" alt="" srcset="">
              <img v-else-if="scope.row.type == 'jpg' || scope.row.type == 'png'" class="fileIcon" src="../../assets/images/image.png" alt="" srcset="">
              <img v-else-if="scope.row.type == 'docx' || scope.row.type == 'doc'" class="fileIcon" src="../../assets/images/word.svg" alt="" srcset="">
              <img v-else-if="scope.row.type == 'xls' || scope.row.type == 'xlsx'" class="fileIcon" src="../../assets/images/excel.svg" alt="" srcset="">
              <img v-else-if="scope.row.type == 'ppt' || scope.row.type == 'pptx'" class="fileIcon" src="../../assets/images/ppt.svg" alt="" srcset="">
              <img v-else-if="scope.row.type == 'pdf'" class="fileIcon" src="../../assets/images/pdf.svg" alt="" srcset="">
              <img v-else-if="scope.row.type == 'txt'" class="fileIcon" src="../../assets/images/txt.svg" alt="" srcset="">
              <img v-else class="fileIcon" src="../../assets/images/file.png" alt="" srcset="">
              {{scope.row.fileName}}
            </div>
          </template>
        </el-table-column>
        <el-table-column label="修改日期" prop="updateTime">
          <template slot-scope="scope">
            <span>{{ parseTime(scope.row.updateTime) }}</span>
          </template>
        </el-table-column>
        <el-table-column label="类型" prop="isFile">
          <template slot-scope="scope">
            {{fileType[scope.row.type] || '文档'}}
          </template>
        </el-table-column>
        <el-table-column label="大小" prop="fileSize">
          <template slot-scope="scope">
            {{scope.row.isFile?Math.ceil(scope.row.fileSize/1024)+' KB':''}}
          </template>
        </el-table-column>
      </el-table>
      <pagination
        v-show="total>0"
        :total="total"
        :pageSizes="[20,30,50]"
        :page.sync="queryForm.pageNum"
        :limit.sync="queryForm.pageSize"
        @pagination="getListFile"
      />
    </div>

    <!-- 右键菜单 -->
    <div id="menu" class="menuDiv el-dropdown-menu">
      <div class="el-dropdown-menu__item" @click.stop="addDirVisible = true">新建文件夹</div>
      <div class="el-dropdown-menu__item" @click.stop="handleUploadClick('uploadDir')">上传文件夹</div>
      <div class="el-dropdown-menu__item" @click.stop="handleUploadClick('uploadFile')">上传文件</div>
      <div class="el-dropdown-menu__item" @click.stop="handleDownload">下载</div>
      <div v-if="cutFileList.length<=0" class="el-dropdown-menu__item" @click.stop="handleCut">剪切</div>
      <div v-if="cutFileList.length>0" class="el-dropdown-menu__item" @click.stop="handleCopy">粘贴</div>
      <div class="el-dropdown-menu__item" @click.stop="handleDelete">删除</div>
      <div v-if="currentRow.fileName" class="el-dropdown-menu__item" @click.stop="handleRename">重命名</div>
      <div v-if="currentRow.isFile" class="el-dropdown-menu__item" @click.stop="handleFileToTable">转存</div>
    </div>

    <!-- 文件预览 -->
    <el-dialog
      v-if="reviewFileVisible"
      :title="currentRow.fileName"
      :visible.sync="reviewFileVisible"
      :destroy-on-close="true"
      width="40%">
        <div style="margin: 5px 0;padding: 8px;height:500px">
          <img v-if="currentRow.type == 'jpg' || currentRow.type == 'png'" :src="reviewFileContent.imgUrl" alt="" style="height:100%;max-width: 100%;"/>
          <video v-if="currentRow.type == 'mp4'" align="center" controls="controls" controlsList="nodownload" disablePictureInPicture autoplay style="width:100%;height: 100%">
            <source :src="reviewFileContent.mp4Url" type="video/mp4"/>
          </video>
          <JsonEditor v-if="(currentRow.type == 'avro' || currentRow.type == 'json') && isJson" :jsonValue="reviewFileContent.fileContent" :readOnly="true"/>
          <el-input
            v-if="currentRow.type !== 'jpg' && currentRow.type !== 'png' && currentRow.type !== 'mp4' && currentRow.type !== 'file' && currentRow.type != 'avro' && currentRow.type != 'json'"
            type="textarea"
            readonly
            resize="none"
            rows="23"
            v-model="reviewFileContent.fileContent"
          ></el-input>
        </div>
      <span slot="footer" class="dialog-footer">
        <el-button @click="reviewFileVisible = false">关闭</el-button>
      </span>
    </el-dialog>
    <!-- 新建文件夹 -->
    <el-dialog
      v-if="addDirVisible"
      title="新建文件夹"
      :visible.sync="addDirVisible"
      :destroy-on-close="true"
      width="30%"
      :before-close="()=>{dirForm={},addDirVisible = false}">
        <el-form :model="dirForm" :rules="rules" ref="dirForm" label-width="100px" class="demo-ruleForm">
          <el-form-item label="文件夹名称" prop="dirName">
            <el-input v-model="dirForm.dirName"></el-input>
          </el-form-item>
        </el-form>
      <span slot="footer" class="dialog-footer">
        <el-button :loading="addDirLoading" @click="dirForm={},addDirVisible = false">关闭</el-button>
        <el-button :loading="addDirLoading" type="primary" @click="handleAddDir">确定</el-button>
      </span>
    </el-dialog>
    <!-- 上传失败文件展示 -->
    <el-dialog
      v-if="errorVisible"
      title="文件上传结果"
      :visible.sync="errorVisible"
      :close-on-click-modal="false"
      width="40%">
      <el-table
        :data="errorFilesData"
        style="width: 100%">
        <el-table-column type="index" width="50"></el-table-column>
        <el-table-column prop="fileName" label="文件名称" show-overflow-tooltip width="200"></el-table-column>
        <el-table-column
          prop="status"
          label="状态"
          width="100">
          <template slot-scope="scope">
            <el-tag :type="scope.row.status=='success'?'success':'danger'">{{scope.row.status=='success'?'成功':'失败'}}</el-tag>
          </template>
        </el-table-column>
        <el-table-column prop="errorMsg" label="失败原因" show-overflow-tooltip></el-table-column>
      </el-table>
      <span slot="footer" class="dialog-footer">
        <el-button @click="errorVisible = false">关闭</el-button>
      </span>
    </el-dialog>

    <!-- 删除失败文件展示 -->
    <el-dialog
      v-if="errorDeleteVisible"
      title="文件删除结果"
      :visible.sync="errorDeleteVisible"
      :close-on-click-modal="false"
      width="40%">
      <el-table
        :data="errorFilesData"
        style="width: 100%" @selection-change="handleSelectionChange">
        <el-table-column type="selection" width="55"></el-table-column>
        <el-table-column prop="fileName" label="文件名称" show-overflow-tooltip width="200"></el-table-column>
        <el-table-column
          prop="status"
          label="状态"
          width="100">
          <template slot-scope="scope">
            <el-tag :type="scope.row.status=='success'?'success':'danger'">{{scope.row.status=='success'?'成功':'失败'}}</el-tag>
          </template>
        </el-table-column>
        <el-table-column prop="errorMsg" label="失败原因" show-overflow-tooltip></el-table-column>
      </el-table>
      <span slot="footer" class="dialog-footer">
        <el-button :loading="delLoading" @click="errorDeleteVisible = false">关闭</el-button>
        <el-button :loading="delLoading" type="primary" @click="handleDeleteBatch">强制删除</el-button>
      </span>
    </el-dialog>

    <!-- 重命名 -->
    <el-dialog
      v-if="renameVisible"
      title="重命名"
      :visible.sync="renameVisible"
      :destroy-on-close="true"
      width="30%"
      :before-close="()=>{renameForm={},renameVisible = false}">
        <el-form :model="renameForm" :rules="renameRules" ref="renameForm" label-width="100px" class="demo-ruleForm">
          <el-form-item label="原名称">
            <el-input v-model="currentRow.fileName" readonly></el-input>
          </el-form-item>
          <el-form-item label="重命名" prop="reName">
            <el-input v-model="renameForm.reName"></el-input>
          </el-form-item>
        </el-form>
      <span slot="footer" class="dialog-footer">
        <el-button :loading="renameLoading" @click="renameForm={},renameVisible = false">关闭</el-button>
        <el-button :loading="renameLoading" type="primary" @click="handleRenameComfire">确定</el-button>
      </span>
    </el-dialog>
    <!-- 转存 -->
    <el-dialog
      v-if="ftoTVisible"
      title="转存"
      :visible.sync="ftoTVisible"
      :destroy-on-close="true"
      width="30%"
      :before-close="()=>{ftoTForm={},ftoTVisible = false}">
        <el-form :model="ftoTForm" :rules="ftoTRules" ref="ftoTForm" label-width="100px" class="demo-ruleForm">
          <el-form-item label="catalog名称" prop="catalog">
            <el-select
              style="width: 100%"
              v-model="ftoTForm.catalog"
              value-key="id"
              @change="()=>{$set(ftoTForm,'databaseName',''),databaseList()}">
              <el-option v-for="(item,index) in catalogDataO" :key="'ca'+index" :label="item.id" :value="item">
                <div>
                  <el-tag size="small" :type="item.types=='hadoop'?'success':''">{{ item.types }}</el-tag>
                  <span style="margin-left:10px">{{item.id}}</span>
                  <span style="margin-left:10px">({{item.descs?item.descs:item.id}})</span>
                </div>
              </el-option>
            </el-select>
          </el-form-item>
          <el-form-item label="库" prop="databaseName">
            <el-select
              style="width: 100%"
              v-model="ftoTForm.databaseName"
              @change="(val)=>{changeDb(val)}">
              <el-option v-for="(item,index) in dbList" :key="'db'+index" :label="item" :value="item"></el-option>
            </el-select>
          </el-form-item>
          <el-form-item label="表" prop="tableName">
            <el-input v-model="ftoTForm.tableName"></el-input>
          </el-form-item>
          <el-form-item label="分隔符" prop="splitStr">
            <el-input v-model="ftoTForm.splitStr" placeholder="请输入分隔符" style="width:100%" />
            <div style="font-size: 12px;color: #999;">确认实际数据中包含该分隔字符，若不包含请修改分隔符</div>
          </el-form-item>
          <el-form-item label="包含表头" prop="isHead">
            <el-radio v-model="ftoTForm.isHead" label="0">否</el-radio>
            <el-radio v-model="ftoTForm.isHead" label="1">是</el-radio>
          </el-form-item>
        </el-form>
      <span slot="footer" class="dialog-footer">
        <el-button :loading="ftoTLoading" @click="ftoTForm={},ftoTVisible = false">关闭</el-button>
        <el-button :loading="ftoTLoading" type="primary" @click="handleftoTComfire">确定</el-button>
      </span>
    </el-dialog>
  </div>
</template>

<script>
import { catalogList } from "@/api/catalog";
import { databaseList, fileToTableData } from '@/api/dbAndTable.js'
import {
  listFile,
  reviewMP4AndPic,
  catFile,
  batchUploadFileToHdfs,
  fastDownloadHdfsFile,
  fastDownloadHdfsDir,
  checkAndDeleteFile,
  batchDeleteFile,
  addDir,
  moveFile,
  renameFile
} from "@/api/unstructuredData"
import JsonEditor from '@/components/codeEditor/JsonEditor.vue'
export default {
  name: "UnstructuredData",
  components: { JsonEditor },
  data() {
    return {
      // 遮罩层
      loading: true,
      // catalog数据
      currentCatalog:{},
      catalogData:[],
      fileType:{
        'file':'文件夹',
        'jpg':'图片',
        'png':'图片',
        'mp4':'视频'
      },
      // file表
      fileData:[],
      queryForm:{
        pageNum:1,
        pageSize:20,
        dirPath:'/'
      },
      total:0,
      currentRow:{},
      reviewFileVisible:false,
      reviewFileContent:{
        fileContent:'',
        imgUrl:'',
        mp4Url:''
      },
      isJson:false,
      isImport:false,
      fileList:[],
      addDirVisible:false,
      dirForm:{},
      rules: {
        name: [
          { required: true, message: '请输入文件夹名称', trigger: 'blur' },
        ],
      },
      addDirLoading:false,
      uploadPercen:0,
      isChange:0,
      errorVisible:false,
      errorFilesData:[],
      errorDeleteVisible:false,
      multipleSelection: [],
      delLoading:false,
      // 剪切/粘贴
      cutFileList:[],
      cutFilePath:'',

      // 返回上一级
      showPathBtn:true,

      // 重命名
      renameVisible: false,
      renameForm:{},
      renameLoading:false,
      renameRules: {
        reName: [
          { required: true, message: '请输入名称', trigger: 'blur' },
        ],
      },

      // 转存
      catalogDataO:[],
      dbList:[],
      ftoTVisible: false,
      ftoTForm:{},
      ftoTLoading:false,
      ftoTRules: {
        catalog: [
          { required: true, message: '请选择catalog', trigger: 'change' },
        ],
        databaseName: [
          { required: true, message: '请选择库', trigger: 'change' },
        ],
        tableName: [
          { required: true, message: '请输入表名称', trigger: 'blur' },
        ],
        splitStr:[
          { required: true, message: '请输入分隔符', trigger: 'blur' },
        ],
      },
    };
  },
  created() {
    this.getCatalogList()
    window.document.addEventListener('click', () => {
      let menu = document.querySelector('#menu')
      if(menu) {
        menu.style.display = 'none'
      }
    }, false)
    var queryFilePath = this.$route.query.filePath
    if(queryFilePath) {
      var data = queryFilePath.slice(1).split('/')
      var pathList = data.splice(0,data.length-1)
      this.queryForm.dirPath = '/' + pathList.join('/')
      console.log('路由跳转',queryFilePath,this.queryForm.dirPath);
    }
  },
  methods: {
    //
    handlePathBtnClick(index) {
      var data = this.queryForm.dirPath.slice(1).split('/')
      var pathList = data.splice(0,index+1)
      this.queryForm.dirPath = '/' + pathList.join('/')
      this.handleQuery()
    },
    /** 查询Catalog列表 */
    getCatalogList() {
      this.loading = true;
      catalogList({pageNum:1,pageSize:1000}).then(response => {
        this.catalogData = response.data.list?.filter(item=>item.types=='file');
        this.catalogDataO = response.data.list?.filter(item=>item.types!='file');
        var queryCatalog = this.$route.query.catalog
        if(queryCatalog ) {
          this.currentCatalog = this.catalogData.filter(item=>item.id == queryCatalog)[0]
          console.log('路由跳转',queryCatalog,this.currentCatalog);
        }else {
          this.currentCatalog = this.catalogData[0]
        }
        this.getListFile()
      }).finally(()=>{
        this.loading = false
      })
    },
    // 获取库
    databaseList() {
      databaseList({...this.ftoTForm.catalog}).then(res=>{
        this.dbList = res.data
      })
    },
    changeDb(val) {
      console.log(val);
      this.$set(this.ftoTForm,'databaseName',val)
    },
    handleQuery() {
      this.getListFile()
    },
    resetQuery() {
      this.queryForm = {
        pageNum:1,
        pageSize:20,
        dirPath:this.queryForm.dirPath
      }
      this.getListFile()
      this.cutFileList = []
    },
    // 查询文件列表
    getListFile() {
      this.loading = true
      listFile({
        catalogId:this.currentCatalog.id,
        hdfsUrl:this.currentCatalog.hdfsurl,
        files:[],
        fileNames:[],
        fileName:"",
        ...this.queryForm
      }).then(res=>{
        if(res.code == 200) {
          this.fileData = res.data.data?res.data.data.map(item=>{
            item.selected = false
            item.cuted = false
            item.type = !item.isFile?'file':item.fileName.split('.')[item.fileName.split('.').length-1]
            return item
          }):[]
          this.total = res.data.total
        }
      }).finally(()=>{
        this.loading = false
      })
    },
    // 文件双击事件，如果是文件夹进入下一级，如果是文件则预览
    handleRowDbClick(row) {
      this.currentRow = JSON.parse(JSON.stringify(row))
      if(!row.isFile) {
        this.queryForm.dirPath += this.queryForm.dirPath=='/'? row.fileName : '/'+row.fileName
        this.queryForm.pageNum = 1
        this.getListFile()
      }else {
        this.handleReviewFile()
      }
    },
    // 文件右键菜单
    handleRightClick(row,column,event) {
      this.currentRow = row?JSON.parse(JSON.stringify(row)):{}
      event.preventDefault();
      // if(row.isFile) {
        let menu = document.querySelector('#menu')
        // 根据鼠标点击位置进行定位
        menu.style.left = event.clientX - 200 + 'px'
        menu.style.top = event.clientY - 85 + 'px'
        // 显示菜单
        menu.style.display = 'block'
        menu.style.zIndex = '1000'
      // }
      event.stopPropagation();
    },
    handleRightClickTable(event) {
      this.currentRow = {}
      let menu = document.querySelector('#menu')
      // 根据鼠标点击位置进行定位
      menu.style.left = event.clientX - 200 + 'px'
      menu.style.top = event.clientY - 85 + 'px'
      // 显示菜单
      menu.style.display = 'block'
      menu.style.zIndex = '1000'
    },
    // 返回上一级
    handleBack(){
      var lastIndex = this.queryForm.dirPath.lastIndexOf('/');
      this.queryForm.dirPath = this.queryForm.dirPath.substr(0, lastIndex)?this.queryForm.dirPath.substr(0, lastIndex):'/';
      this.getListFile()
    },
    handleUploadClick(ele) {
      var uploadEle = document.getElementById(ele)
      uploadEle.click()
    },
    // 文件上传
    handleUpload(ele) {
      var fileList = document.getElementById(ele).files;
      let formData = new FormData()
      for(var i=0; i<fileList.length; i++) {
        formData.append('files', fileList[i])
      }
      formData.append('catalogId', this.currentCatalog.id)
      formData.append('hdfsUrl', this.currentCatalog.hdfsurl)
      formData.append('dirPath', this.queryForm.dirPath)
      this.loading = true
      batchUploadFileToHdfs(formData,(e)=>{
        this.uploadPercen = parseInt(e.loaded/e.total*100)
      }).then(res=>{
        if(res.code == 200) {
          if(res.data.errorFiles && res.data.errorFiles.length>0) {
            this.errorFilesData = []
            for(var key in res.data.errorMessage) {
              this.errorFilesData.push({
                fileName:key,
                errorMsg:res.data.errorMessage[key],
                status:'error'
              })
            }
            res.data.successFiles?res.data.successFiles.map(item=>{
              this.errorFilesData.push({
                fileName:item,
                errorMsg:'-',
                status:'success'
              })
            }):[]
            this.errorVisible = true
          }else {
            this.$message.success('上传成功')
          }
          this.getListFile()
        }
      }).finally(()=>{
        this.loading  =false
      })
    },
    // 文件下载
    handleDownload() {
      var selectedList = this.fileData.filter(item=>item.selected)
      var fileNames = selectedList.length>0?selectedList.map(item=>{return item.fileName}):[this.currentRow.fileName]
      if(fileNames.length>0) {
        this.$message({
          type:'success',
          duration:2000,
          message:'下载任务异步执行中，请耐心等待...'
        })
        fileNames.map(item=>{
          if(this.currentRow.isFile) {
            fastDownloadHdfsFile({
              catalogId:this.currentCatalog.id,
              hdfsUrl:this.currentCatalog.hdfsurl,
              filePath:this.queryForm.dirPath,
              fileName:item
            }).then(res=>{
              const blob = new Blob([res])
              const link = document.createElement('a')
              link.style.display = 'none'
              link.href = URL.createObjectURL(blob)
              link.download = item // 下载的文件名
              document.body.appendChild(link)
              link.click()
              document.body.removeChild(link)
            })
          }else {
            fastDownloadHdfsDir({
              catalogId:this.currentCatalog.id,
              hdfsUrl:this.currentCatalog.hdfsurl,
              filePath:this.queryForm.dirPath,
              fileName:item
            }).then(res=>{
              const blob = new Blob([res])
              const link = document.createElement('a')
              link.style.display = 'none'
              link.href = URL.createObjectURL(blob)
              link.download = item+'.zip' // 下载的文件名
              document.body.appendChild(link)
              link.click()
              document.body.removeChild(link)
            })
          }
        })
      }else {
        this.$message.warning('未选中需要下载的文件！')
      }
    },
    // 行点击事件
    handleRowClick(row,column,event) {
      var index = this.fileData.indexOf(row)
      row.selected = !row.selected
      this.$set(this.fileData,index,row)
    },
    tableRowClassName({row, rowIndex}) {
      if (row.cuted) {
        return 'cut-row';
      }
      if (row.selected) {
        return 'select-row';
      }
      return '';
    },
    handleDelete() {
      var selectedList = this.fileData.filter(item=>item.selected)
      var fileNames = selectedList.length>0?selectedList.map(item=>{return item.fileName}):[this.currentRow.fileName].filter(item=>item)
      if(fileNames.length>0) {
        this.$confirm('是否确认删除选中文件'+fileNames.join('、')+'?', '提示', {
          confirmButtonText: '确定',
          cancelButtonText: '取消',
          type: 'warning'
        }).then(() => {
          this.loading = true
          checkAndDeleteFile({
            catalogId:this.currentCatalog.id,
            hdfsUrl:this.currentCatalog.hdfsurl,
            filePath:this.queryForm.dirPath,
            fileNames:fileNames,
          }).then(res=>{
            if(res.code == 200) {
              if(res.data.errorFiles && res.data.errorFiles.length>0) {
                this.errorFilesData = []
                for(var key in res.data.errorMessage) {
                  this.errorFilesData.push({
                    fileName:key,
                    errorMsg:res.data.errorMessage[key],
                    status:'error'
                  })
                }
                res.data.successFiles?res.data.successFiles.map(item=>{
                  this.errorFilesData.push({
                    fileName:item,
                    errorMsg:'-',
                    status:'success'
                  })
                }):[]
                this.errorDeleteVisible = true
              }else {
                this.$message.success('删除文件'+fileNames.join('、')+'成功')
              }
              this.getListFile()
            }
          }).finally(()=>{
            this.loading = false
            this.isChange++
          })
        })
      }else {
        this.$message.warning('未选中需要删除的文件！')
      }
    },
    handleSelectionChange(val) {
      this.multipleSelection = val;
    },
    handleDeleteBatch() {
      if(this.multipleSelection.length>0) {
        var fileNames = this.multipleSelection.map(item=>{return item.fileName})
        this.$confirm('是否确认删除选中文件'+fileNames.join('、')+'?', '提示', {
          confirmButtonText: '确定',
          cancelButtonText: '取消',
          type: 'warning'
        }).then(() => {
          this.delLoading = true
          batchDeleteFile({
            catalogId:this.currentCatalog.id,
            hdfsUrl:this.currentCatalog.hdfsurl,
            filePath:this.queryForm.dirPath,
            fileNames:fileNames,
          }).then(res=>{
            if(res.code == 200) {
              this.$message.success('删除文件'+fileNames.join('、')+'成功')
              this.getListFile()
            }
          }).finally(()=>{
            this.delLoading = false
            this.errorDeleteVisible = false
          })
        })
      }else {
        this.$message.warning('未选中需要删除的文件！')
      }
    },
    handleReviewFile() {
      this.loading = true
      if(this.currentRow.type == 'jpg' || this.currentRow.type == 'png' || this.currentRow.type == 'mp4') {
        reviewMP4AndPic({
          catalogId:this.currentCatalog.id,
          hdfsUrl:this.currentCatalog.hdfsurl,
          filePath:this.queryForm.dirPath,
          fileName:this.currentRow.fileName
        }).then(res=>{
          if(this.currentRow.type == 'jpg' || this.currentRow.type == 'png') {
            this.reviewFileContent.imgUrl = res?window.URL.createObjectURL(res):''
          }else {
            this.reviewFileContent.mp4Url = res?window.URL.createObjectURL(res):''
          }
          this.reviewFileVisible = true
        }).finally(()=>{
          this.loading = false
        })
        // var host = window.location.host
        // var host = "http://192.168.10.41:8080"
        // var catalogId = this.currentCatalog.id
        // var hdfsUrl = this.currentCatalog.hdfsurl
        // var filePath = this.queryForm.dirPath
        // var fileName = this.currentRow.fileName
        // if(this.currentRow.type == 'jpg' || this.currentRow.type == 'png') {
        //   this.reviewFileContent.imgUrl = `http://192.168.10.41:8080/iceberg/unStructured/reviewMP4AndPic/${catalogId}?catalogId=${catalogId}&hdfsUrl=${hdfsUrl}&filePath=${filePath}&fileName=${fileName}`
        // }else {
        //   this.reviewFileContent.mp4Url = `http://192.168.10.41:8080/iceberg/unStructured/reviewMP4AndPic/${catalogId}?catalogId=${catalogId}&hdfsUrl=${hdfsUrl}&filePath=${filePath}&fileName=${fileName}`
        // }
        // this.reviewFileVisible = true
        // this.loading = false
      }else{
        catFile({
          catalogId:this.currentCatalog.id,
          hdfsUrl:this.currentCatalog.hdfsurl,
          files:[],
          fileNames:[],
          fileName:this.currentRow.fileName,
          dirPath:this.queryForm.dirPath,
        }).then(res=>{
          if(res.code == 200) {
            if(this.currentRow.type == 'avro' || this.currentRow.type == 'json') {
              // 格式化json代码
              try {
                this.reviewFileContent.fileContent = JSON.stringify(JSON.parse(res.data),null,'\t')
                this.isJson = true
              }catch(err) {
                this.reviewFileContent.fileContent = res.data
                this.isJson = false
              }
            }else {
              this.reviewFileContent.fileContent = res.data
            }
            this.reviewFileVisible = true
          }
        }).finally(()=>{
          this.loading = false
        })
      }
    },
    // 新建文件夹
    handleAddDir() {
      this.$refs['dirForm'].validate((valid) => {
          if (valid) {
            this.addDirLoading = true
            addDir({
              catalogId:this.currentCatalog.id,
              hdfsUrl:this.currentCatalog.hdfsurl,
              dirPath:this.queryForm.dirPath,
              dirName:this.dirForm.dirName
            }).then(res=>{
              if(res.code == 200) {
                this.$message.success('新建文件夹成功')
                this.getListFile()
              }
            }).finally(()=>{
              this.addDirLoading = false
              this.addDirVisible = false
            })
          } else {
            console.log('error submit!!');
            return false;
          }
        });
    },
    handleCut() {
      var selectedList = this.fileData.filter(item=>item.selected)
      var fileNames = selectedList.length>0?selectedList.map(item=>{return item.fileName}):[this.currentRow.fileName].filter(item=>item)

      this.cutFileList = JSON.parse(JSON.stringify(fileNames))
      this.cutFilePath = this.queryForm.dirPath

      this.fileData.map(row=>{
        if(this.cutFileList.indexOf(row.fileName)!=-1) {
          row.cuted = true
        }else {
          row.cuted = false
        }
        return row
      })

      console.log(this.cutFileList,this.cutFilePath);
    },
    handleCopy() {
      moveFile({
        catalogId:this.currentCatalog.id,
        hdfsUrl:this.currentCatalog.hdfsurl,
        filePath:this.cutFilePath,
        fileNames:this.cutFileList,
        dirPath:this.queryForm.dirPath
      }).then(res=>{
        if(res.code == 200) {
          this.$message.success(res.msg)
          this.getListFile()
          this.cutFileList = []
          this.cutFilePath = ''
        }
      })
    },

    // 重命名
    handleRename() {
      this.renameForm = {}
      this.renameVisible = true
    },
    handleRenameComfire() {
      this.$refs['renameForm'].validate((valid) => {
        if (valid) {
          this.renameLoading = true
          renameFile({
            catalogId:this.currentCatalog.id,
            hdfsUrl:this.currentCatalog.hdfsurl,
            dirPath:this.queryForm.dirPath,
            fileName:this.currentRow.fileName,
            reName:this.renameForm.reName
          }).then(res=>{
            if(res.code == 200) {
              this.$message.success('重命名成功')
              this.getListFile()
            }
          }).finally(()=>{
            this.renameLoading = false
            this.renameVisible = false
          })
        } else {
          console.log('error submit!!');
          return false;
        }
      });
    },
    // 转存 file=>table
    handleFileToTable() {
      this.ftoTForm = {splitStr:',',isHead:'0'}
      this.ftoTVisible = true
    },
    handleftoTComfire() {
      this.$refs['ftoTForm'].validate((valid) => {
        if (valid) {
          var data = {
            "icebergTableParam":{
              "id":this.ftoTForm.catalog.id,
              "hdfsurl":this.ftoTForm.catalog.hdfsurl,
              "types":this.ftoTForm.catalog.types,
              "hiveurl":this.ftoTForm.catalog.hiveurl,
              "databaseName":this.ftoTForm.databaseName,
              "tableName":this.ftoTForm.tableName,
            },
            "icebergFileParam":{
              "catalogId":this.currentCatalog.id,
              "hdfsUrl":this.currentCatalog.hdfsurl,
              "filePath":this.queryForm.dirPath + '/' + this.currentRow.fileName
            },
            "splitStr":this.ftoTForm.splitStr,
            "isHead":this.ftoTForm.isHead
          }
          console.log(data);
          fileToTableData({...data}).then(res=>{

          })
          this.ftoTVisible = false
          this.$notify({
            title: '转存结果',
            dangerouslyUseHTMLString: true,
            message: `非结构化文件转储到表数据任务进行中，表数据查看[${this.ftoTForm.catalog.id}->${this.ftoTForm.databaseName}->${this.ftoTForm.tableName}]`,
            duration: 0,
          });
        } else {
          console.log('error submit!!');
          return false;
        }
      });
    }
  }
};
</script>
<style lang="scss" scoped>
.menuDiv {
  display: none;
  position: absolute;
  background: #fff;
  width: 120px;
}
::v-deep .el-table td.el-table__cell {
  border:unset!important;
}
::v-deep .el-table--medium .el-table__cell {
  padding: 4px 0!important;
}
.el-dropdown-menu__item {
    line-height: 28px;
    padding: 0px 16px;
}
input[type=file] {
  width: 98px!important;
  height: 32px!important;
  opacity: 0;
  position: absolute;
  top: 0;
  left: 0;
  cursor: pointer;
}
::v-deep .el-table .select-row {
  background: #f5f7fa;
}
::v-deep .el-table .cut-row {
  background: #fff;
  color: #999;
}
.fileNameArea{
  display: flex;
  align-items: center;
}
.fileIcon {
  width: 14px;
  height: auto;
  margin-right: 10px;
}
</style>
