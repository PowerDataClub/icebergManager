<template>
<div>
   <div class="main-area">
    <!-- 查询条件 -->
     <el-form :model="queryParams" ref="queryForm" size="small" :inline="true" v-show="showSearch">
      <el-form-item label="快照ID" prop="snapshotId">
        <el-select
          v-model="queryParams.snapshotId"
          placeholder="请选择快照ID"
          clearable
          style="width: 240px"
        >
          <el-option v-for="(item,index) in SnapshotData" :key="index" :label="item.snapshotId" :value="item.snapshotId"/>
        </el-select>
      </el-form-item>
      <el-form-item label="指定时间" prop="asOfTime">
        <el-date-picker
          v-model="queryParams.asOfTime"
          type="datetime"
          value-format="timestamp"
          style="width: 240px"
          placeholder="获取指定时间前的数据">
        </el-date-picker>
      </el-form-item>
      <el-form-item>
        <el-button type="primary" icon="el-icon-search" size="mini" @click="handleQuery">搜索</el-button>
        <el-button icon="el-icon-refresh" size="mini" @click="resetQuery">刷新</el-button>
      </el-form-item>
    </el-form>
    <el-row :gutter="10" class="mb8">
      <el-col :span="1.5">
        <el-button
          type="primary"
          plain
          icon="el-icon-plus"
          size="mini"
          @click="handleAddData"
        >新增</el-button>
      </el-col>
      <el-col :span="1.5">
        <el-button
          type="danger"
          plain
          icon="el-icon-delete"
          size="mini"
          :disabled="multiple"
          @click="handleDeleteData"
        >删除</el-button>
      </el-col>
      <!-- <el-col :span="1.5">
        <el-upload
          action=""
          :multiple="false"
          :show-file-list="false"
          :http-request="handleUpload"
          accept=".xls,.xlsx"
          name="file">
          <el-button :loading="isImport" size="mini" type="primary" icon="el-icon-upload2">导入</el-button>
        </el-upload>
      </el-col>
      <el-col :span="1.5">
        <el-button type="primary" size="mini" @click="unloadingForm={splitStr:',',isHead:'0'};unloadingVisible=true">
          <i class="el-icon-sort" style="transform: rotate(90deg);"></i>
          转存
        </el-button>
      </el-col> -->
      <right-toolbar :showSearch.sync="showSearch" @queryTable="getTableData"></right-toolbar>
    </el-row>
    <el-table
      style="width:100%"
      :data="tableData"
      v-loading="loading"
      size="small"
      height="425"
      @selection-change="handleSelectionChange"
      @row-dblclick="handleRowDblclick"
      >
      <el-table-column type="selection" width="55" align="center"/>
      <el-table-column v-for="(item,index) in columnList" :key="index" :label="item" :prop="item" show-overflow-tooltip >
        <template slot-scope="scope">
          <div v-if="!isAdd && dataForm.rowIndex == scope.$index">
            <el-input
              size="mini"
              v-model="dataForm[item]"
              :placeholder="'请输入'+ item + '值' +
                (columnTypeObj[item].indexOf('map<')!=-1
                  ? '，例：map(xxx,xxx)'
                  : (columnTypeObj[item].indexOf('list<')!=-1 ? '，例：array(xxx,xxx,xx,...)' :''))"/>
          </div>
          <div v-else>
           {{scope.row[item]}}
          </div>
        </template>
      </el-table-column>
      <el-table-column label="操作" align="center" width="120" v-if="!isAdd">
        <template slot-scope="scope">
          <el-button
            v-if="dataForm.rowIndex == scope.$index"
            size="mini"
            type="text"
            icon="el-icon-check"
            @click="submitDataForm"
          >保存</el-button>
          <el-button
            v-if="dataForm.rowIndex == scope.$index"
            size="mini"
            type="text"
            icon="el-icon-close"
            @click="isAdd=true,dataForm={},oldData={},dataVisible = false"
          >取消</el-button>
        </template>
      </el-table-column>
      <!-- <el-table-column label="操作" align="center" width="80">
        <template slot-scope="scope">
          <el-button
            size="mini"
            type="text"
            icon="el-icon-edit"
            @click="handleUpdateData(scope.row)"
          >修改</el-button>
        </template>
      </el-table-column> -->
    </el-table>
    <pagination
      v-show="total>0"
      :total="total"
      :layout="'total, sizes'"
      :pageSizes="[10,100,200,500]"
      :page.sync="page.pageNum"
      :limit.sync="page.pageSize"
      @pagination="handleQuery"
    />
  </div>
  <!-- 数据配置对话框 -->
  <el-dialog :title="isAdd?'插入数据':'编辑数据'" :visible.sync="dataVisible" v-if="dataVisible" width="600px" append-to-body>
    <el-form :model="dataForm" label-width="120px">
      <el-form-item v-for="(item,index) in columnList" :key="index" :label="item+'：'" >
        <el-input
          v-model="dataForm[item]"
          :placeholder="'请输入'+ item + '值' +
            (columnTypeObj[item].indexOf('map<')!=-1
              ? '，例：map(xxx,xxx)'
              : (columnTypeObj[item].indexOf('list<')!=-1 ? '，例：array(xxx,xxx,xx,...)' :''))"/>
      </el-form-item>
    </el-form>
    <div slot="footer" class="dialog-footer">
      <el-button :loading="addLoading" type="primary" @click="submitDataForm">确 定</el-button>
      <el-button :loading="addLoading" @click="dataVisible=false">取 消</el-button>
    </div>
  </el-dialog>
  <!-- 数据配置对话框 -->
  <el-dialog title="数据转存" :visible.sync="unloadingVisible" v-if="unloadingVisible" width="600px" append-to-body>
    <el-form ref="unloadingForm" :model="unloadingForm" :rules="unloadingFormRules" label-width="120px">
      <el-form-item label="catalog名称" prop="catalog">
        <el-select v-model="unloadingForm.catalog" placeholder="请选择catalog名称" style="width:100%" value-key="id">
          <el-option v-for="(item,index) in catalogList" :key="index" :label="item.id" :value="item">
            <div>
              <el-tag size="small" :type="item.types=='hadoop'?'success':''">{{ item.types}}</el-tag>
              <span style="margin-left:10px">{{item.id}}</span>
              <span style="margin-left:10px">({{item.descs?item.descs:item.id}})</span>
            </div>
          </el-option>
        </el-select>
      </el-form-item>
      <el-form-item label="目标文件路径" prop="filePath">
        <el-input v-model="unloadingForm.filePath" placeholder="请输入目标路径" style="width:100%" />
      </el-form-item>
      <el-form-item label="分隔符" prop="splitStr">
        <el-input v-model="unloadingForm.splitStr" placeholder="请输入分隔符" style="width:100%" />
        <div style="font-size: 12px;color: #999;">确认实际数据中不包含该分隔字符，若包含请修改分隔符</div>
      </el-form-item>
      <el-form-item label="包含表头" prop="isHead">
        <el-radio v-model="unloadingForm.isHead" label="0">否</el-radio>
        <el-radio v-model="unloadingForm.isHead" label="1">是</el-radio>
      </el-form-item>
    </el-form>
    <div slot="footer" class="dialog-footer">
      <el-button type="primary" @click="handleUnloading">确 定</el-button>
      <el-button @click="unloadingVisible=false">取 消</el-button>
    </div>
  </el-dialog>
</div>

</template>
<script>
import {
  getTableData,
  tableSnapshot,
  tableColumns,
  getDataBySnapshotId,
  getDataByTime,
  addTableData,
  updateTableData,
  delTableData,
  importTableData,
  tableDataToFile} from '@/api/dbAndTable.js'
export default {
  name: '',
  components: {},
  props:{
    currentCatalog:{
      type:Object,
      required:true
    },
    currentTable:{
      type:Object,
      required:true
    },
    catalogList:{
      type:Array,
      required:false,
      default:[]
    }
  },
  data() {
    return {
      loading:false,
      // 显示搜索条件
      showSearch: true,
      SnapshotData:[],
      queryParams:{},
      columnList:[],
      columnTypeObj:{},
      tableData:[],
      page:{
        pageNum:1,
        pageSize:10
      },
      total:0,
      multiple:true,
      selectListToDel:[],
      dataVisible:false,
      isAdd:true,
      dataForm:{},
      oldData:{},
      isImport:false,
      addLoading:false,

      // 转存
      unloadingVisible:false,
      unloadingForm:{},
      unloadingFormRules:{
        catalog:[
          { required: true, message: '请选择catalog名称', trigger: 'change' },
        ],
        filePath:[
          { required: true, message: '请输入目标文件路径', trigger: 'blur' },
        ],
        splitStr:[
          { required: true, message: '请输入分隔符', trigger: 'blur' },
        ],
      },
    }
  },
  computed: {},
  watch: {},
  created() {
    this.getTableColumns()
    this.getTableData()
    if(this.showSearch) {
      this.getTableSnapshot()
    }
  },
  methods: {
    getTableSnapshot() {
      tableSnapshot({
        ...this.currentCatalog,
        databaseName:this.currentTable.pLabel,
        tableName:this.currentTable.label
      }).then(res=>{
        this.SnapshotData = res.data
      })
    },
    getTableColumns() {
      tableColumns({
        ...this.currentCatalog,
        databaseName:this.currentTable.pLabel,
        tableName:this.currentTable.label
      }).then(res=>{
        this.columnList = res.data.map(item=>{
          this.columnTypeObj[item.columnName] = item.dataType
          return item.columnName
        })
      })
    },
    handleQuery() {
      if(this.queryParams.snapshotId && this.queryParams.asOfTime){
        this.$message.warning('查询条件需唯一')
      }else if(this.queryParams.snapshotId) {
        this.getDataBySnapshotId()
      }else if(this.queryParams.asOfTime) {
        this.getDataByTime()
      }else {
        this.getTableData()
      }
    },
    resetQuery() {
      this.queryParams = {}
      this.getTableData()
    },
    // 数据预览
    getTableData(){
      this.loading = true
      getTableData({
        ...this.currentCatalog,
        databaseName:this.currentTable.pLabel,
        tableName:this.currentTable.label,
        ...this.page
      }).then(res=>{
        this.render(res.data)
      }).catch(err=>{
        this.render({
          list:[],
          total:0
        })
      })
    },
    // 查询指定快照数据
    getDataBySnapshotId() {
      this.loading = true
      getDataBySnapshotId({
        ...this.currentCatalog,
        databaseName:this.currentTable.pLabel,
        tableName:this.currentTable.label,
        snapshotId:this.queryParams.snapshotId,
        ...this.page
      }).then(res=>{
        this.render(res.data)
      }).catch(err=>{
        this.render({
          list:[],
          total:0
        })
      })
    },
    // 查询指定时间前的数据
    getDataByTime() {
      this.loading = true
      getDataByTime({
        ...this.currentCatalog,
        databaseName:this.currentTable.pLabel,
        tableName:this.currentTable.label,
        asOfTime:this.queryParams.asOfTime,
        ...this.page
      }).then(res=>{
        this.render(res.data)
      }).catch(err=>{
        this.render({
          list:[],
          total:0
        })
      })
    },

    // 渲染表格
    render(data) {
      this.loading = false
      this.tableData = data.list
      this.tableData.map((item,index)=>{
        item.rowIndex = index
        return item
      })
      this.total = data.total
    },

    // 插入数据
    handleAddData() {
      this.isAdd = true
      this.dataForm = {}
      this.dataVisible = true
    },
    // 修改数据
    handleUpdateData(row) {
      this.isAdd = false
      this.dataForm = JSON.parse(JSON.stringify(row))
      this.dataVisible = true
    },
    handleRowDblclick(row, column, event) {
      console.log(111,row, column, event);
      this.isAdd = false
      this.oldData = JSON.parse(JSON.stringify(row))
      this.dataForm = JSON.parse(JSON.stringify(row))
    },
    // 提交新增/修改
    submitDataForm() {
      if(this.isAdd) {
        this.addLoading = true
        addTableData({
          ...this.currentCatalog,
          databaseName:this.currentTable.pLabel,
          tableName:this.currentTable.label,
          addData:[{...this.dataForm}]
        }).then(res=>{
          if(res.code == 200) {
            this.dataVisible = false
            this.handleQuery()
            this.$emit('dataChange')
          }
        }).finally(()=>{
          this.addLoading = false
        })
      }else {
        this.loading = true
        updateTableData({
          ...this.currentCatalog,
          databaseName:this.currentTable.pLabel,
          tableName:this.currentTable.label,
          oldData:this.oldData,
          newData:this.dataForm
        }).then(res=>{
          if(res.code == 200) {
            this.isAdd = true
            this.handleQuery()
            this.$emit('dataChange')
          }
        }).finally(()=>{
          this.loading = false
        })
      }
    },
    // 多选框选中数据
    handleSelectionChange(selection) {
      this.selectListToDel = selection
      this.multiple = !selection.length
    },
    // 删除数据
    handleDeleteData() {
      this.$modal.confirm('是否确认删除选中的的数据？').then(()=>{
        this.loading = true
        var selectListToDel = []
        this.selectListToDel.map(item=>{
          let obj = {}
          for(var key in item) {
            if(key != 'rowIndex') {
              obj[key] = item[key]
            }
          }
          selectListToDel.push(obj)
        })
        delTableData({
          ...this.currentCatalog,
          databaseName:this.currentTable.pLabel,
          tableName:this.currentTable.label,
          addData:selectListToDel
        }).then((res) => {
          if(res.code == 200) {
            this.$modal.msgSuccess("删除成功");
            this.handleQuery()
            this.$emit('dataChange')
          }
        }).finally(()=>{
          this.loading = false
        })
      }).catch((err) => {
          console.log(err);
      });
    },
    // 导入
    handleUpload(file) {
      let data = new FormData()
      data.append('file',file.file)
      for(var key in this.currentCatalog) {
        data.append(key,this.currentCatalog[key])
      }
      data.append('databaseName',this.currentTable.pLabel)
      data.append('tableName',this.currentTable.label)
      this.loading = true
      importTableData(data).then(res=>{
        if(res.code == 200) {
          this.$modal.msgSuccess('导入成功')
          this.handleQuery()
          this.$emit('dataChange')
        }
      }).finally(()=>{
        this.loading = false
      })
    },

    // 保存到本地
    handleUnloading() {
      this.$refs['unloadingForm'].validate((valid) => {
        if (valid) {
          var filePath = this.unloadingForm.filePath[0]!='/'? '/' + this.unloadingForm.filePath : this.unloadingForm.filePath
          tableDataToFile({
            "icebergTableParam":{
              "id":this.currentCatalog.id,
              "hdfsurl":this.currentCatalog.hdfsurl,
              "types":this.currentCatalog.types,
              "hiveurl":this.currentCatalog.hiveurl,
              "databaseName":this.currentTable.pLabel,
              "tableName":this.currentTable.label,
            },
            "icebergFileParam":{
              "catalogId":this.unloadingForm.catalog.id,
              "hdfsUrl":this.unloadingForm.catalog.hdfsurl,
              "filePath":filePath
            },
            "splitStr":this.unloadingForm.splitStr,
            "isHead":this.unloadingForm.isHead
          }).then(res=>{

          })
          this.unloadingVisible = false
          const notify = this.$notify({
            title: '转存结果',
            dangerouslyUseHTMLString: true,
            message: `表数据转储到非结构化文件任务进行中，非结构化数据文件查看[${this.unloadingForm.catalog.id}]
            <span style="color:#4a7bff;cursor: pointer;">${filePath}</span>`,
            duration: 0,
          });
          notify.$el.querySelector('span').onclick = () =>{
            this.$router.push({
              path:'/unstructuredData',
              query:{catalog:this.unloadingForm.catalog.id,'filePath':filePath}
            })
            notify.close()
          }
        } else {
          console.log('error submit!!');
          return false;
        }
      });
    },
  }
}
</script>
<style lang="scss">
.main-area {
  width: 100%;
  padding: 10px;
}
 .el-dropdown-link {
    cursor: pointer;
    color: #409EFF;
  }
  .el-icon-arrow-down {
    font-size: 12px;
  }
  .el-table__body-wrapper {
    height: calc(100% - 40px)!important;
    overflow: auto!important;
  }
</style>
