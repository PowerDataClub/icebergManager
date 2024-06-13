<template>
  <div>
    <el-form :model="queryParams" ref="queryForm" size="small" :inline="true" >
      <el-form-item label="执行sql" prop="sqlstr">
        <el-input v-model="queryParams.sqlstr" placeholder="请输入sql名称" style="width:160px" clearable @keyup.enter.native="handleQuery" />
      </el-form-item>
      <el-form-item label="执行人" prop="creater">
        <el-input v-model="queryParams.creater" placeholder="请输入执行人" style="width:160px" clearable @keyup.enter.native="handleQuery" />
      </el-form-item>
      <el-form-item label="执行结果" prop="status">
        <el-select v-model="queryParams.status" placeholder="请选择执行结果" style="width:160px" clearable @keyup.enter.native="handleQuery">
          <el-option label="运行中" value="0"></el-option>
          <el-option label="失败" value="1"></el-option>
          <el-option label="成功" value="2"></el-option>
        </el-select>
      </el-form-item>
      <el-form-item>
        <el-button type="primary" icon="el-icon-search" size="mini" @click="handleQuery">搜索</el-button>
        <el-button icon="el-icon-refresh" size="mini" @click="resetQuery">重置</el-button>
      </el-form-item>
    </el-form>

    <el-table height="306"
    v-loading="loading"
    :data="historyList"
    @sort-change="handleSortChange"
    @row-click="handleRowClick"
    >
      <el-table-column label="执行sql" prop="sqlstr" width="300" show-overflow-tooltip/>
      <el-table-column label="catalog名称" prop="catalog" width="100" show-overflow-tooltip/>
      <el-table-column label="库" prop="databaseName" width="100" show-overflow-tooltip/>
      <el-table-column label="执行引擎" prop="execType" width="100" show-overflow-tooltip/>
      <el-table-column label="运行模式" prop="mode" width="100" show-overflow-tooltip/>
      <el-table-column label="耗时" prop="exectimes"  width="160" />
      <el-table-column label="执行结果" prop="status" width="120">
        <template slot-scope="scope">
          <el-tag :type="scope.row.status==0 ? 'primary' : (scope.row.status==2 ? 'success' : 'danger')">
            {{scope.row.status==0 ? '运行中' : (scope.row.status==2 ? '成功' : '失败')}}
          </el-tag>
        </template>
      </el-table-column>
      <el-table-column label="日志信息" prop="other1">
        <template slot-scope="scope">
          <el-tooltip class="item" effect="dark" :content="scope.row.other1" placement="top">
            <div class="tooltipDiv">{{scope.row.other1}}</div>
          </el-tooltip>
        </template>
      </el-table-column>
      <el-table-column label="执行人" prop="creater" width="120"/>
      <el-table-column label="执行时间" prop="createtime" width="160" sortable>
        <template slot-scope="scope">
          <span>{{ parseTime(scope.row.createtime) }}</span>
        </template>
      </el-table-column>

    </el-table>

    <pagination
      v-show="total>0"
      :total="total"
      :page.sync="queryParams.pageNum"
      :limit.sync="queryParams.pageSize"
      @pagination="getList"
    />
  </div>
</template>

<script>
import { sqlLogHistory } from "@/api/sqlClient";

export default {
  name: "HistoryManage",
  data() {
    return {
      // 遮罩层
      loading: true,
      // 总条数
      total: 0,
      // 角色表格数据
      historyList: [],
      queryParams: {
        pageNum: 1,
        pageSize: 10,
        orderBy:undefined,
        isAsc:undefined
      },
    };
  },
  created() {
    this.getList();
  },
  methods: {
    handleSortChange(data) {
      this.queryParams.orderBy = data.prop
      this.queryParams.isAsc = data.order=="descending"?'desc':'asc'
      this.getList()
    },
    /** 查询sql列表 */
    getList() {
      this.loading = true;
      sqlLogHistory(this.queryParams).then(response => {
          this.historyList = response.data.list?response.data.list.map(item=>{
            var other2 = item.other2?item.other2.split(','):[]
            item.catalog = other2[0]
            item.databaseName = other2[1]
            item.execType = other2[2]
            item.mode = other2[3]
            return item
          }):[]
          this.total = response.data.total;
          this.loading = false;
        }
      );
    },
    /** 搜索按钮操作 */
    handleQuery() {
      this.queryParams.pageNum = 1;
      this.getList();
    },
    /** 重置按钮操作 */
    resetQuery() {
      this.resetForm("queryForm");
      this.handleQuery();
    },
    handleRowClick(row) {
      this.$emit('handleHistoryRowClick',row)
    }
  }
};
</script>
<style lang="scss" scoped>
.tooltipDiv {
    width: 100%;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
  }
</style>
