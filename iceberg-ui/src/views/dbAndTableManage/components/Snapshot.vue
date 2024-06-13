<template>
  <div class="main-area">
    <el-table v-loading="loading" :data="SnapshotData" height="calc(100vh - 359px)">
      <el-table-column label="序号" type="index" width="50"></el-table-column>
      <el-table-column label="快照ID" prop="snapshotId"  width="180"/>
      <el-table-column label="操作类型" prop="operation" width="80"/>
      <el-table-column label="新增文件数" prop="addFileCount" width="100"/>
      <el-table-column label="删除文件数" prop="deleteFileCount" width="100"/>
      <el-table-column label="新增文件大小(KB)" prop="addFileSize"/>
      <el-table-column label="删除文件大小(KB)" prop="deleteFileSize"/>
      <el-table-column label="当前版本快照" align="center" prop="isCurrent" width="100">
        <template slot-scope="scope">
          <el-tag :type="scope.row.isCurrent?'success':'info'">{{ scope.row.isCurrent?'是':'否' }}</el-tag>
        </template>
      </el-table-column>
      <el-table-column label="当前实际数据量" prop="records"/>
      <el-table-column label="创建时间" align="center" prop="commitTime" width="160">
        <template slot-scope="scope">
          <span>{{ parseTime(scope.row.commitTime) }}</span>
        </template>
      </el-table-column>
      <el-table-column label="操作" align="center">
        <template slot-scope="scope">
          <el-dropdown>
            <span class="el-dropdown-link">
              <i class="el-icon-more"></i>
            </span>
            <el-dropdown-menu slot="dropdown">
              <el-dropdown-item @click.native="handleSetCurrentSnapshot(scope.row)"><i class="el-icon-sort" style="transform:rotate(90deg)"></i>切换快照</el-dropdown-item>
              <el-dropdown-item @click.native="handleRollbackSnapshot(scope.row)"><i class="el-icon-refresh-left"></i>快照回滚</el-dropdown-item>
              <el-dropdown-item :disabled="scope.row.isCurrent" @click.native="handleCherryPickSnapshot(scope.row)"><i class="el-icon-cherry"></i>cherrypick快照</el-dropdown-item>
            </el-dropdown-menu>
          </el-dropdown>
        </template>
      </el-table-column>
    </el-table>
  </div>
</template>
<script>
import { tableSnapshot, setCurrentSnapshot, rollbackSnapshot, cherryPickSnapshot } from '@/api/dbAndTable.js'
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
      loading:false,
      SnapshotData:[],
      currentSnapshot:{},
    }
  },
  computed: {},
  watch: {},
  created() {
    this.getTableSnapshot()
  },
  methods: {
    getTableSnapshot() {
      this.loading = true
      tableSnapshot({
        ...this.currentCatalog,
        databaseName:this.currentTable.pLabel,
        tableName:this.currentTable.label
      }).then(res=>{
        this.SnapshotData = res.data
      }).finally(()=>{
        this.loading = false
      })
    },
    // 切换快照
    handleSetCurrentSnapshot(row) {
      this.$confirm('是否确认执行切换快照操作?', '提示', {
        confirmButtonText: '确定',
        cancelButtonText: '取消',
        type: 'warning'
      }).then(() => {
        setCurrentSnapshot({
          ...this.currentCatalog,
          databaseName:this.currentTable.pLabel,
          tableName:this.currentTable.label,
          snapshotId:row.snapshotId
        }).then(res=>{
          if(res.code == 200) {
            this.$modal.msgSuccess(res.msg);
            this.getTableSnapshot()
          }
        })
      }).catch(() => {
        this.$message({
          type: 'info',
          message: '已取消操作'
        });          
      });
    },
    // 快照回滚
    handleRollbackSnapshot(row) {
      this.$confirm('是否确认执行快照回滚操作?', '提示', {
        confirmButtonText: '确定',
        cancelButtonText: '取消',
        type: 'warning'
      }).then(() => {
        rollbackSnapshot({
          ...this.currentCatalog,
          databaseName:this.currentTable.pLabel,
          tableName:this.currentTable.label,
          snapshotId:row.snapshotId
        }).then(res=>{
          if(res.code == 200) {
            this.$modal.msgSuccess(res.msg);
            this.getTableSnapshot()
          }
        })
      }).catch(() => {
        this.$message({
          type: 'info',
          message: '已取消操作'
        });          
      });
    },
    // cherrypick快照
    handleCherryPickSnapshot(row) {
      this.$confirm('是否确认执行cherrypick快照操作?', '提示', {
        confirmButtonText: '确定',
        cancelButtonText: '取消',
        type: 'warning'
      }).then(() => {
        cherryPickSnapshot({
          ...this.currentCatalog,
          databaseName:this.currentTable.pLabel,
          tableName:this.currentTable.label,
          snapshotId:row.snapshotId
        }).then(res=>{
          if(res.code == 200) {
            this.$modal.msgSuccess(res.msg);
            this.getTableSnapshot()
          }
        })
      }).catch(() => {
        this.$message({
          type: 'info',
          message: '已取消操作'
        });          
      });
    },
  }
}
</script>
<style lang="scss" scoped>
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
::v-deep.el-dropdown-menu--medium .el-dropdown-menu__item {
  padding: 0px 12px;
  border-bottom: 1px solid #f1f1f1;
}
</style>