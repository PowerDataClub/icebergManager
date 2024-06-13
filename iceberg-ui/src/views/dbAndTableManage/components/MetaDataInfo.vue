<template>
<div>
   <div class="main-area">
    <!-- 查询条件 -->
    <el-form :model="queryParams" ref="queryForm" size="small" :inline="true">
      <el-form-item label="文件名">
        <el-input v-model="queryParams.fileName"></el-input>
      </el-form-item>
      <el-form-item>
        <el-button type="primary" icon="el-icon-search" size="mini" @click="getMetadataFiles">搜索</el-button>
        <el-button icon="el-icon-refresh" size="mini" @click="resetQuery">重置</el-button>
      </el-form-item>
    </el-form>

    <el-table size="small" :data="tableData" v-loading="loading" height="460">
      <el-table-column label="文件名" prop="fileName" show-overflow-tooltip/>
      <el-table-column label="操作" align="center" width="100">
        <template slot-scope="scope">
          <el-button
            size="mini"
            type="text"
            icon="el-icon-view"
            @click="handleLookDetails(scope.row)"
          >查看</el-button>
        </template>
      </el-table-column>
    </el-table>
    <pagination
      v-show="total>0"
      :total="total"
      :layout="'total, sizes, prev, pager, next'"
      :pageSizes="[10,100,200,500]"
      :page.sync="page.pageNum"
      :limit.sync="page.pageSize"
      @pagination="getMetadataFiles"
    />
  </div>
  <!-- 数据配置对话框 -->
  <el-dialog :title="'查看文件 ' + currentRow.fileName" :visible.sync="viewVisible" v-if="viewVisible" width="40%" append-to-body>
    <div v-if="(currentRow.fileType == 'avro' || currentRow.fileType == 'json') && isJson" style="height:500px">
      <JsonEditor  :jsonValue="fileContent" :readOnly="true" />
    </div>
    <el-input
        v-else
        rows="20"
        type="textarea"
        placeholder="暂无日志信息"
        v-model="fileContent"
        readonly  
      />
      <span slot="footer" class="dialog-footer">
        <el-button @click="viewVisible = false">关闭</el-button>
      </span>
  </el-dialog>
</div>
 
</template>
<script>
import { getMetadataFiles, getMetadata } from '@/api/dbAndTable.js'
import JsonEditor from '@/components/codeEditor/JsonEditor.vue'
export default {
  name: '',
  components: {JsonEditor},
  props:{
    currentCatalog:{
      type:Object,
      required:true
    },
    currentTable:{
      type:Object,
      required:true
    },
  },
  data() {
    return {
      loading:false,
      queryParams:{
        fileName:''
      },
      tableData:[],
      page:{
        pageNum:1,
        pageSize:10
      },
      total:0,
      viewVisible:false,
      currentRow:{},
      fileContent:'',
      isJson:false
    }
  },
  computed: {},
  watch: {},
  created() {
    this.getMetadataFiles()
  },
  methods: {
    resetQuery() {
      this.queryParams = {fileName:''}
      this.getMetadataFiles()
    },
    // 数据预览
    getMetadataFiles(){
      this.loading = true
      getMetadataFiles({
        ...this.queryParams,
        ...this.currentCatalog,
        databaseName:this.currentTable.pLabel,
        tableName:this.currentTable.label,
        ...this.page
      }).then(res=>{
        if(res.code == 200) {
          this.tableData = res.data.data.map(item=>{
            return {
              fileName:item,
              fileType:item.split('.')[item.split('.').length-1]
            }
          })
          this.total = res.data.total
        }else {
          this.tableData = []
          this.total = 0
          this.$message.error(res.msg)
        }
      }).finally(()=>{
        this.loading = false
      })
    },

    // 渲染表格
    handleLookDetails(row) {
      this.currentRow = row
      this.loading = true
      getMetadata({
        ...this.currentCatalog,
        databaseName:this.currentTable.pLabel,
        tableName:this.currentTable.label,
        fileName:row.fileName,
        ...this.page
      }).then(res=>{
        if(res.code == 200) {
          try {
            this.fileContent = JSON.stringify(JSON.parse(res.data),null,'\t')
            this.isJson = true
          }catch(err) {
            this.fileContent = res.data
            this.isJson = false
          }
          this.viewVisible = true
        }else {
          this.$message.error(res.msg)
        }
      }).finally(()=>{
        this.loading = false
      })
    }
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