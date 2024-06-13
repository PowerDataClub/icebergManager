<template>
  <div class="app-container" v-loading="loadingF" style="position: relative;">
    <el-form :model="form" :rules="rules" ref="form" size="small" :inline="true">
      <el-form-item label="catalog名称" prop="catalog">
        <el-select size="small" v-model="form.currentCatalog" value-key="id" @change="getDatabaseList" style="width:80%;margin-right:8px;">
          <el-option v-for="(item,index) in catalogData" :key="index" :label="item.id" :value="item"></el-option>
        </el-select>
      </el-form-item>
      <el-form-item label="库" prop="databaseName">
        <el-select size="small" v-model="form.databaseName" style="width:80%;margin-right:8px;">
          <el-option v-for="(item,index) in databaseData" :key="index" :label="item.id" :value="item.label"></el-option>
        </el-select>
      </el-form-item>
      <el-form-item label="执行引擎" prop="execType">
        <el-select size="small" v-model="form.execType" style="width:80%;margin-right:8px;">
          <el-option label="flink" value="flink"></el-option>
          <el-option label="spark" value="spark"></el-option>
        </el-select>
      </el-form-item>
      <el-form-item label="运行模式" prop="mode">
        <el-select size="small" v-model="form.mode" style="width:80%;margin-right:8px;">
          <el-option label="local" value="local"></el-option>
          <el-option label="yarn" value="yarn"></el-option>
        </el-select>
      </el-form-item>
      <el-form-item>
        <el-button type="primary" icon="el-icon-caret-right" size="mini" :loading="loading" @click="handleSqlExecute">运行</el-button>
        <el-button plain type="primary" icon="el-icon-brush" size="mini" @click="handleFormat">格式化查询语句</el-button>
        <el-button icon="el-icon-refresh" size="mini" @click="resetQuery">重置</el-button>
        <el-button icon="el-icon-info" size="mini" @click="icebergMdVisible=true">说明</el-button>
        <el-button plain type="primary" icon="el-icon-setting" size="mini" @click="sqlManageVisible=true">sql管理</el-button>
      </el-form-item>
    </el-form>
    <div class="sqlArea">
      <keep-alive>
        <SqlEditor ref="seditor" v-model="sqlValue" />
      </keep-alive>
      <el-button
        style="position: absolute;top: 0;right: 0;z-index:9"
        plain
        type="primary"
        icon="el-icon-circle-check"
        size="mini"
        @click="handleAddSql"
        title="保存sql语句">保存sql</el-button>
        <div class="shortcutsArea">
          <div class="shortcutsTitle">SQL示例</div>
          <div class="sqlShortcuts">
            <div v-for="(item,index) in sqlShortcuts" :key="index" @click="handleInitSql(item.sql)" class="ssItem">{{item.title}}</div>
          </div>
        </div>
    </div>

    <div>
      <el-tabs type="border-card" v-model="activeName" @tab-click="handleClickTab">
        <el-tab-pane label="执行历史" name="first">
          <HistoryManage :key="isChangeHis" @handleHistoryRowClick="handleHistoryRowClick"/>
        </el-tab-pane>
        <el-tab-pane label="查询结果" name="second" v-if="showResult">
          <keep-alive>
            <el-tabs v-loading="loading" element-loading-text="sql执行中，请耐心等待...">
              <el-tab-pane
                v-for="(item,index) in resultData" :key="index">
                <span slot="label" :title="item.sqlKey">{{item.sqlKey}}</span>
                <div style="display:flex;align-items:center;justify-content: space-between;margin-bottom:10px;">
                  <div style="font-szie:12px;color:#666">
                    执行耗时：{{item.execTime}}
                  </div>
                  <el-button type="primary" icon="el-icon-view" size="mini" @click="handleLookLog(item.log)">查看日志</el-button>
                </div>

                <div v-if="item.sqlDataType == 'string'" style="height:340px;overflow-y:auto">
                  {{item.sqlData}}
                </div>
                <div v-else>
                  <el-table :data="item.sqlData" style="width: 100%" height="340">
                    <el-table-column v-for="(it,i) in item.sqlDataKeys" :key="i"
                      :prop="it"
                      :label="it"
                      show-overflow-tooltip>
                    </el-table-column>
                  </el-table>
                </div>
              </el-tab-pane>
            </el-tabs>
          </keep-alive>
        </el-tab-pane>
      </el-tabs>
    </div>
    <el-dialog
      title="查看日志"
      :visible.sync="lodVisible"
      width="40%">
      <!-- <div v-html="log"></div> -->
      <el-input
        rows="20"
        type="textarea"
        placeholder="暂无日志信息"
        v-model="log"
        readonly
      />
      <span slot="footer" class="dialog-footer">
        <el-button @click="lodVisible = false">关闭</el-button>
      </span>
    </el-dialog>

    <el-dialog
      title="说明"
      :visible.sync="icebergMdVisible"
      v-if="icebergMdVisible"
      width="50%">
        <div style="width:100%;border:1px solid #C0C4CC">
          <Iceberg/>
        </div>
      <span slot="footer" class="dialog-footer">
        <el-button @click="icebergMdVisible = false">关闭</el-button>
      </span>
    </el-dialog>

    <el-dialog
      title="sql管理"
      :visible.sync="sqlManageVisible"
      v-if="sqlManageVisible"
      width="50%">
        <SqlManage @handleStrart="handleStrart"/>
      <span slot="footer" class="dialog-footer">
        <el-button @click="sqlManageVisible = false">关闭</el-button>
      </span>
    </el-dialog>

    <el-dialog title="保存sql语句" :visible.sync="addSqlVisible" width="500px" append-to-body>
      <el-form ref="addForm" :model="addForm" :rules="addRules" label-width="100px">
        <el-form-item label="sql名称" prop="name">
          <el-input v-model="addForm.name" placeholder="请输入sql名称" />
        </el-form-item>
        <el-form-item label="执行sql" prop="execsql">
          <el-input type="textarea" v-model="addForm.execsql" placeholder="请输入执行sql" />
        </el-form-item>
      </el-form>
      <div slot="footer" class="dialog-footer">
        <el-button :loading="addBtnLoading" type="primary" @click="submitForm">确 定</el-button>
        <el-button :loading="addBtnLoading" @click="cancel">取 消</el-button>
      </div>
    </el-dialog>
  </div>
</template>

<script>
import { catalogList } from "@/api/catalog";
import { sqlExecute } from "@/api/sqlClient";
import { databaseList } from '@/api/dbAndTable.js'
import SqlEditor from '@/components/codeEditor/SqlEditor.vue'
import SqlManage from './SqlManage.vue'
import Iceberg from './Iceberg.vue'
import { sqlLogAdd } from "@/api/sqlClient";
import HistoryManage from "./HistoryManage.vue"

export default {
  name: "sqlClient",
  components:{ SqlEditor, Iceberg, SqlManage, HistoryManage },
  data() {
    return {
      // 遮罩层
      loadingF: false,
      loading: false,
      form: {
        currentCatalog:{},
        databaseName:'',
        execType:'spark',
        mode:'local'
      },
      catalogData:[],
      databaseData:[],
      // 表单校验
      rules: {
        currentCatalog: [
          { required: true, message: "catalog不允许为空", trigger: "change" }
        ],
        databaseName: [
          { required: true, message: "库不允许为空", trigger: "change" }
        ]
      },
      sqlValue:'',
      resultData:{},
      lodVisible:false,
      log:"",
      icebergMdVisible:false,
      sqlManageVisible:false,

      // 保存sql语句
      addSqlVisible:false,
      addBtnLoading:false,
      addForm: {},
      addRules: {
        name: [
          { required: true, message: "sql名称不能为空", trigger: "blur" }
        ],
        execsql: [
          { required: true, message: "执行sql不能为空", trigger: "blur" }
        ]
      },

      // 查询历史
      activeName:'first',
      isChangeHis:0,
      showResult:false,

      // sql语句示例
      sqlShortcuts:[
        {title:'CreateTable',sql:'create table db_name.table_name ( id int, name string) using iceberg tblproperties ("table.props" = "val");'},
        {title:'DeleteTable',sql:"drop table db_name.table_name;"},
        {title:'EditTable',sql:"alter table db_name.table_name add column data int ;alter table db_name.table_name alter column data bigint ;alter table db_name.table_name drop column data;"},
        {title:'AlterProperties',sql:"alter table db_name.table_name set tblproperties ('comment' = 'A table comment.');"},
        {title:'ShowDatabases',sql:"show databases;"},
        {title:'ShowTables',sql:"show tables;"},
        {title:'Describe',sql:"desc db_name.table_name;"},
      ]
    };
  },
  created() {
    this.getCatalogList();
  },
  methods: {
    /** 查询Catalog列表 */
    getCatalogList() {
      catalogList({pageNum:1,pageSize:1000}).then(response => {
        this.catalogData = response.data.list?.filter(item=>item.types!='file');
        this.form.currentCatalog = this.catalogData[0]
        this.getDatabaseList()
      })
    },

    /** 查询库 */
    getDatabaseList() {
      this.loadingF = true
      databaseList({...this.form.currentCatalog}).then(res=>{
        this.databaseData = res.data?res.data.map(item=>{
          return {
            id:item,
            label:item,
            type:'database',
          }
        }):[]
        this.form.databaseName = this.databaseData[0].label
      }).catch(err=>{
        this.databaseData = []
      }).finally(()=>{
        this.loadingF = false
      })
    },
    resetQuery() {
      this.form.currentCatalog = this.catalogData[0]
      this.form.databaseName = ''
      this.$refs.seditor.setValue('')
      this.resultData = []
    },

    // 格式化sql语句
    handleFormat() {
      this.$refs.seditor.formatSql()
    },

    /** 运行按钮 */
    handleSqlExecute () {
      this.$refs["form"].validate(valid => {
        if (valid) {
          if (this.$refs.seditor.getValue().replace(/(^\s*) | (\s*$)/g, '') === '') {
            this.$message.warning('请输入sql语句')
          } else {
            console.log('result',this.form,this.$refs.seditor.getValue());
            this.loading = true
            this.showResult = true
            this.activeName = 'second'
            sqlExecute({
              ...this.form.currentCatalog,
              execType:this.form.execType,
              mode:this.form.mode,
              databaseName:this.form.databaseName,
              execSql:this.$refs.seditor.getValue()
            }).then(res=>{
              if(res.code == 200) {
                this.resultData = []
                var reg = new RegExp("\r\n","g")
                for(var key in res.data) {
                  let obj = {
                    sqlKey:key,
                    sqlData:res.data[key].data,
                    sqlDataKeys:typeof res.data[key].data == 'string'?[]:Object.keys(res.data[key].data[0]),
                    sqlDataType: typeof res.data[key].data,
                    execTime: res.data[key].execTime,
                    log: res.data[key].log
                  }
                  this.resultData.push(obj)
                }
              }
            }).finally(()=>{
              this.loading = false
            })
          }
        }
      });
    },
    handleLookLog(log) {
      console.log(log)
      this.log = log
      this.lodVisible = true
    },

    // 自动带入sql
    handleStrart(sql) {
      this.$refs.seditor.setValue(sql)
      this.sqlManageVisible = false
    },

    // 保存sql语句
    handleAddSql() {
      var currentSql = this.$refs.seditor.getValue()
      if(currentSql.trim()) {
        this.addSqlVisible = true
        this.addForm = {}
        this.addForm.execsql = this.$refs.seditor.getValue()
      }else {
        this.$modal.msgWarning("当前sql语句为空！");
      }

    },
    /** 提交按钮 */
    submitForm() {
      this.$refs["addForm"].validate(valid => {
        if (valid) {
          this.addBtnLoading = true
          sqlLogAdd({...this.addForm}).then(response => {
            this.$modal.msgSuccess("保存成功");
            this.addSqlVisible = false;
          }).finally(()=>{
            this.addBtnLoading = false
          })
        }
      });
    },
    // 取消按钮
    cancel() {
      this.addSqlVisible = false;
      this.addForm = {}
    },

    // 切换tab
    handleClickTab(tab) {
      if(this.activeName == 'first') {
        this.isChangeHis++
      }
    },

    handleHistoryRowClick(row) {
      this.$refs.seditor.setValue(row.sqlstr)
      this.form.currentCatalog = this.catalogData.filter(item=>item.id == row.catalog)[0]
      setTimeout(() => {
        this.form.databaseName = row.databaseName
      }, 100);
      this.form.execType = row.execType
      this.form.mode = row.mode
    },

    handleInitSql(sql) {
      this.$refs.seditor.setValue(sql)
    }
  }
};
</script>
<style lang="scss">
  .sqlArea {
    margin: 10px 0 20px 0;
    border: 2px solid rgb(234, 234, 234);
    height: 204px;
    width: calc(100% - 200px);
    position: relative;
  }
  .shortcutsArea {
    margin: 10px 0 20px 0;
    border: 2px solid rgb(234, 234, 234);
    height: 204px;
    width: 200px;
    position: absolute;
    top: -12px;
    right: -200px;
    font-size: 13px;
    .shortcutsTitle {
      padding: 6px 10px;
      border-bottom: 1px solid #f1f1f1;
      color: #606266;
    }
    .sqlShortcuts {
      padding: 5px 10px;
      overflow: auto;
      height: calc(100% - 28px);
      .ssItem {
        padding: 4px 0;
        color: #4a7bff;
        cursor: pointer;
      }
    }
  }

  .el-tabs__item {
    max-width: 300px!important;
    overflow: hidden!important;
    text-overflow: ellipsis!important;
  }
  .el-tabs__nav {
    height:40px!important
  }
  .el-tabs__content {
    min-height: 370px;
  }

</style>
