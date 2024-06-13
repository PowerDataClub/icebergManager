<template>
  <div>
    <el-form :model="queryParams" ref="queryForm" size="small" :inline="true" v-show="showSearch">
      <el-form-item label="sql名称" prop="name">
        <el-input v-model="queryParams.name" placeholder="请输入sql名称" style="width:160px" clearable @keyup.enter.native="handleQuery" />
      </el-form-item>
      <el-form-item label="执行sql" prop="execsql">
        <el-input v-model="queryParams.execsql" placeholder="请输入执行sql语句" style="width:160px" clearable @keyup.enter.native="handleQuery"/>
      </el-form-item>
      <el-form-item label="保存人" prop="creater">
        <el-input v-model="queryParams.creater" placeholder="请输入保存人" style="width:160px" clearable @keyup.enter.native="handleQuery"/>
      </el-form-item>
      <el-form-item>
        <el-button type="primary" icon="el-icon-search" size="mini" @click="handleQuery">搜索</el-button>
        <el-button icon="el-icon-refresh" size="mini" @click="resetQuery">重置</el-button>
      </el-form-item>
    </el-form>

    <el-row :gutter="10" class="mb8">
      <el-col :span="1.5">
        <el-button
          type="primary"
          plain
          icon="el-icon-plus"
          size="mini"
          @click="handleAdd"
        >新增</el-button>
      </el-col>
      <el-col :span="1.5">
        <el-button
          type="primary"
          plain
          icon="el-icon-caret-right"
          size="mini"
          :disabled="multiple"
          @click="handleStrart"
        >执行</el-button>
      </el-col>
      <el-col :span="1.5">
        <el-button
          type="danger"
          plain
          icon="el-icon-delete"
          size="mini"
          :disabled="multiple"
          @click="handleDelete"
        >删除</el-button>
      </el-col>
      <right-toolbar :showSearch.sync="showSearch" @queryTable="getList"></right-toolbar>
    </el-row>

    <el-table height="500" v-loading="loading" :data="sqlList" @selection-change="handleSelectionChange" @sort-change="handleSortChange">
      <el-table-column type="selection" width="55" align="center" />
      <el-table-column label="sql名称" prop="name" width="120" />
      <el-table-column label="执行sql" prop="execsql"/>
      <el-table-column label="保存人" prop="creater" width="120"/>
      <el-table-column label="保存时间" prop="createtime" width="150" sortable>
        <template slot-scope="scope">
          <span>{{ parseTime(scope.row.createtime) }}</span>
        </template>
      </el-table-column>
      <el-table-column label="操作" class-name="small-padding fixed-width" width="150">
        <template slot-scope="scope">
          <el-button
            size="mini"
            type="text"
            icon="el-icon-edit"
            @click="handleUpdate(scope.row)"
          >修改</el-button>
          <el-button
            size="mini"
            type="text"
            icon="el-icon-caret-right"
            @click="handleStrart(scope.row)"
          >执行</el-button>
          <el-button
            size="mini"
            type="text"
            icon="el-icon-delete"
            @click="handleDelete(scope.row)"
          >删除</el-button>
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

    <!-- 添加或修改对话框 -->
    <el-dialog :title="title" :visible.sync="open" width="500px" append-to-body>
      <el-form ref="form" :model="form" :rules="rules" label-width="100px">
        <el-form-item label="sql名称" prop="name">
          <el-input v-model="form.name" placeholder="请输入sql名称" />
        </el-form-item>
        <el-form-item label="执行sql" prop="execsql">
          <el-input type="textarea" v-model="form.execsql" placeholder="请输入执行sql" />
        </el-form-item>
      </el-form>
      <div slot="footer" class="dialog-footer">
        <el-button :loading="editBtnLoading" type="primary" @click="submitForm">确 定</el-button>
        <el-button :loading="editBtnLoading" @click="cancel">取 消</el-button>
      </div>
    </el-dialog>

  
  </div>
</template>

<script>
import { sqlLogList, sqlLogDelete, sqlLogAdd, sqlLogModify} from "@/api/sqlClient";

export default {
  name: "sqlManage",
  data() {
    return {
      // 遮罩层
      loading: true,
      // 选中数组
      names: [],
      execsqls: [],
      // 非单个禁用
      single: true,
      // 非多个禁用
      multiple: true,
      // 显示搜索条件
      showSearch: true,
      // 总条数
      total: 0,
      // 角色表格数据
      sqlList: [],
      // 弹出层标题
      title: "",
      // 是否显示弹出层
      open: false,
      isAdd:true,
      // 查询参数
      queryParams: {
        pageNum: 1,
        pageSize: 10,
        name: undefined,
        execsql: undefined,
        creater: undefined,
        orderBy:undefined,
        isAsc:undefined
      },
      // 表单参数
      form: {},
      // 表单校验
      rules: {
        name: [
          { required: true, message: "sql名称不能为空", trigger: "blur" }
        ],
        execsql: [
          { required: true, message: "执行sql不能为空", trigger: "blur" }
        ]
      },
      editBtnLoading:false,
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
      sqlLogList(this.queryParams).then(response => {
          this.sqlList = response.data.list;
          this.total = response.data.total;
          this.loading = false;
        }
      );
    },
    // 表单重置
    reset() {
      this.form = {
        name: undefined,
        execsql: undefined,
      };
      this.resetForm("form");
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
    // 多选框选中数据
    handleSelectionChange(selection) {
      this.names = selection.map(item => item.name)
      this.execsqls = selection.map(item => item.execsql)
      this.single = selection.length!=1
      this.multiple = !selection.length
    },
    
    /** 新增按钮操作 */
    handleAdd() {
      this.reset();
      this.isAdd = true
      this.open = true;
      this.title = "添加sql";
    },
    /** 修改按钮操作 */
    handleUpdate(row) {
      this.reset();
      this.form = JSON.parse(JSON.stringify(row))
      this.isAdd = false
      this.open = true;
      this.title = "修改sql";
    },
    /** 提交按钮 */
    submitForm() {
      this.$refs["form"].validate(valid => {
        if (valid) {
          this.editBtnLoading = true
          if (this.isAdd) {
            sqlLogAdd({...this.form}).then(response => {
              this.$modal.msgSuccess("新增成功");
              this.open = false;
              this.getList();
            }).finally(()=>{
              this.editBtnLoading = false
            })
          } else {
            sqlLogModify({...this.form}).then(response => {
              this.$modal.msgSuccess("修改成功");
              this.open = false;
              this.getList();
            }).finally(()=>{
              this.editBtnLoading = false
            })
          }
        }
      });
    },
    // 取消按钮
    cancel() {
      this.open = false;
      this.reset();
    },
    /** 删除按钮操作 */
    handleDelete(row) {
      const names = row.name?[row.name]:this.names;
      this.$modal.confirm('是否确认删除sql名称为"' + names.join(',') + '"的数据项？').then(function() {
        return sqlLogDelete({delNames:names});
      }).then(() => {
        this.getList();
        this.$modal.msgSuccess("删除成功");
      }).catch(() => {});
    },
    /** 执行按钮操作 */
    handleStrart(row) {
      const execsqls = row.execsql?[row.execsql]:this.execsqls;
      var sql = execsqls.join(';')
      this.$emit('handleStrart',sql)
    },
  }
};
</script>