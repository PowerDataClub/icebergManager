<template>
  <div class="main-area">
    <div class="main-area-item">
      <div style="margin-bottom:10px">字段信息</div>
      <el-table v-loading="loadingC" :data="ColumnsData" height="calc(100vh - 400px)">
        <el-table-column label="序号" type="index" width="50"></el-table-column>
        <el-table-column label="字段名称" prop="columnName"/>
        <el-table-column label="字段类型" prop="dataType"/>
        <el-table-column label="必填" prop="isNullable" width="55"> 
          <template slot-scope="scope">
            <el-tag size="mini" :type="scope.row.isNullable==1?'success':'info'">{{ scope.row.isNullable==1?'是':'否' }}</el-tag>
          </template>
        </el-table-column>
        <el-table-column label="字段描述" prop="comment"/>
      </el-table>
    </div>
    <div class="main-area-item">
      <div style="margin-bottom:10px">分区信息</div>
      <el-table v-loading="loadingP" :data="PartitionData" height="calc(100vh - 400px)">
        <el-table-column label="序号" type="index" width="50"></el-table-column>
        <el-table-column label="分区字段" prop="field"/>
        <el-table-column label="来源字段" prop="sourceField"/>
        <el-table-column label="分区规则" prop="transform"/>
      </el-table>
    </div>
    
  </div>
</template>
<script>
import { tableColumns, tablePartition } from '@/api/dbAndTable.js'
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
  },
  data() {
    return {
      loadingC:false,
      loadingP:false,
      ColumnsData:[],
      PartitionData:[]
    }
  },
  computed: {},
  watch: {},
  created() {
    this.getTableColumns()
    this.getTablePartition()
  },
  methods: {
    getTableColumns() {
      this.loadingC = true
      tableColumns({
        ...this.currentCatalog,
        databaseName:this.currentTable.pLabel,
        tableName:this.currentTable.label
      }).then(res=>{
        this.ColumnsData = res.data
      }).finally(()=>{
        this.loadingC = false
      })
    },
    getTablePartition() {
      this.loadingP = true
      tablePartition({
        ...this.currentCatalog,
        databaseName:this.currentTable.pLabel,
        tableName:this.currentTable.label
      }).then(res=>{
        this.PartitionData = res.data
      }).finally(()=>{
        this.loadingP = false
      })
    }
  }
}
</script>
<style lang="scss" scoped>
.main-area {
  width: 100%;
  display: flex;
  .main-area-item {
    flex: 1;
    margin: 10px;
  }
}
</style>