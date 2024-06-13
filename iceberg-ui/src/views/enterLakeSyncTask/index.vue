<template>
  <div class="app-container">
    <el-form :model="queryParams" ref="queryForm" size="small" :inline="true" v-show="showSearch" label-width="100px">
      <el-form-item label="来源种类">
        <el-select
          v-model="queryParams.sourcetypes"
          placeholder="请选择来源种类"
          clearable
          style="width: 240px"
        >
          <el-option label="hdfs" value="1"/>
          <el-option label="hive" value="2"/>
          <el-option label="kafka" value="3"/>
          <el-option label="mysql" value="4"/>
        </el-select>
      </el-form-item>
      <el-form-item label="连接ip">
        <el-input
          v-model="queryParams.sourceip"
          placeholder="请输入连接ip"
          clearable
          style="width: 240px"
          @keyup.enter.native="handleQuery"
        />
      </el-form-item>
      <el-form-item label="数据信息">
        <el-input
          v-model="queryParams.syncdata"
          placeholder="请输入数据信息"
          clearable
          style="width: 240px"
          @keyup.enter.native="handleQuery"
        />
      </el-form-item>
      <el-form-item label="状态">
        <el-select
          v-model="queryParams.status"
          placeholder="请选择status"
          clearable
          style="width: 240px"
        >
          <el-option label="运行中" value="0"/>
          <el-option label="失败" value="1"/>
          <el-option label="成功" value="2"/>
          <el-option label="停止" value="3"/>
        </el-select>
      </el-form-item>
      <el-form-item label="目标catalog">
        <el-select v-model="currentCatalog" value-key="id" @change="getDatabaseList" placeholder="请选择目catalog" clearable style="width: 240px">
          <el-option v-for="(item,index) in catalogData" :key="index" :label="item.id" :value="item">
            <div>
              <el-tag size="small" :type="item.types=='hadoop'?'success':''">{{ item.types}}</el-tag>
              <span style="margin-left:10px">{{item.id}}</span>
              <span style="margin-left:10px">({{item.descs?item.descs:item.id}})</span>
            </div>
          </el-option>
        </el-select>
      </el-form-item>
      <el-form-item label="目标库">
        <el-select v-model="queryParams.distdatabase" placeholder="请选择目标库" clearable style="width: 240px">
          <el-option v-for="(item,index) in databaseData" :key="index" :label="item" :value="item"></el-option>
        </el-select>
      </el-form-item>
      <el-form-item label="目标表">
        <el-input
          v-model="queryParams.disttable"
          placeholder="请输入目标表"
          clearable
          style="width: 240px"
          @keyup.enter.native="handleQuery"
        />
      </el-form-item>
      <el-form-item label="创建者">
        <el-input
          v-model="queryParams.creater"
          placeholder="请输入创建者"
          clearable
          style="width: 240px"
          @keyup.enter.native="handleQuery"
        />
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
      <right-toolbar :showSearch.sync="showSearch" @queryTable="syncTaskList"></right-toolbar>
    </el-row>

    <el-table v-loading="loading" :data="syncTaskData" height="calc(100vh - 392px)" @sort-change="handleSortChange">
      <el-table-column label="任务id" prop="id" width="130" sortable/>
      <el-table-column label="数据来源种类" prop="sourcetypes" width="100">
        <template slot-scope="scope">
          <el-tag size="small" :type="scope.row.sourcetypes=='1'?'success':(scope.row.sourcetypes=='2'?'':'warning')">
            {{ scope.row.sourcetypes=='1'?'hdfs':(scope.row.sourcetypes=='2'?'hive':(scope.row.sourcetypes=='4'?'mysql':'kafka')) }}
          </el-tag>
        </template>
      </el-table-column>
      <el-table-column label="连接ip" prop="sourceip"  sortable/>
      <el-table-column label="数据信息" prop="syncdata" sortable>
        <template slot-scope="scope">
          <el-tooltip class="item" effect="dark" :content="scope.row.syncdata" placement="top">
            <div class="tooltipDiv">{{scope.row.syncdata}}</div>
          </el-tooltip>
        </template>
      </el-table-column>
      <el-table-column label="目标catalog" prop="distcatalogid" sortable/>
      <el-table-column label="目标库" prop="distdatabase"  sortable/>
      <el-table-column label="目标表" prop="disttable" show-overflow-tooltip  sortable/>
      <el-table-column label="状态" prop="status" width="100">
        <template slot-scope="scope">
          <el-tag size="small" :type="statusObj[scope.row.status].type">
            {{statusObj[scope.row.status].name}}
          </el-tag>
        </template>
      </el-table-column>
      <el-table-column label="执行时长" prop="exectimes"  show-overflow-tooltip/>
      <el-table-column label="执行信息" prop="other1" width="200">
        <template slot-scope="scope">
          <el-tooltip class="item" effect="dark" :content="scope.row.other1" placement="top">
            <div class="tooltipDiv">{{scope.row.other1}}</div>
          </el-tooltip>
        </template>
      </el-table-column>
      <el-table-column label="创建人" prop="creater" width="80"/>
      <el-table-column label="创建时间" prop="createtime" width="160" sortable>
        <template slot-scope="scope">
          <span>{{ parseTime(scope.row.createtime) }}</span>
        </template>
      </el-table-column>

      <el-table-column label="操作" align="right" width="120">
        <template slot-scope="scope">
          <el-button
            size="mini"
            type="text"
            icon="el-icon-video-play"
            :disabled="scope.row.status == '0'"
            @click="handleUpdate(scope.row)"
          >执行</el-button>
          <el-button
            size="mini"
            type="text"
            icon="el-icon-video-pause"
            :disabled="scope.row.status !== '0'"
            @click="handleStop(scope.row)"
          >停止</el-button>
        </template>
      </el-table-column>
    </el-table>

    <pagination
      v-show="total>0"
      :total="total"
      :page.sync="queryParams.pageNum"
      :limit.sync="queryParams.pageSize"
      @pagination="syncTaskList"
    />

    <!-- 添加或修改catalog配置对话框 -->
    <el-dialog :title="isAdd?'新增入湖任务':'执行入湖任务'" :visible.sync="open" v-if="open" width="500px" :before-close="cancel" append-to-body :close-on-click-modal="false">
      <el-form ref="form" :model="form" :rules="rules" label-width="140px">
        <el-form-item label="来源种类" prop="sourcetypes">
          <el-select
            v-model="form.sourcetypes"
            placeholder="请选择来源种类"
            style="width: 100%"
            @change="handleChangeType"
          >
            <el-option label="hdfs" value="1"/>
            <el-option label="hive" value="2"/>
            <el-option label="kafka" value="3"/>
            <el-option label="mysql" value="4"/>
          </el-select>
        </el-form-item>
        <el-form-item label="连接ip" prop="sourceip">
          <el-input v-model="form.sourceip" placeholder="请输入连接ip"/>
        </el-form-item>
        <el-form-item label="连接端口" prop="other2" v-if="form.sourcetypes == '4'">
          <el-input v-model="form.other2" placeholder="请输入连接端口"/>
        </el-form-item>
        <el-form-item label="连接账号" prop="other3" v-if="form.sourcetypes == '4'">
          <el-input v-model="form.other3" placeholder="请输入连接账号"/>
        </el-form-item>
        <el-form-item label="连接密码" prop="other4" v-if="form.sourcetypes == '4'">
          <el-input type="password" v-model="form.other4" placeholder="请输入连接密码"/>
        </el-form-item>
        <el-form-item label="迁移类型" prop="syncdataType" v-if="(form.sourcetypes == '2' || form.sourcetypes == '4') && isAdd">
          <el-radio v-model="form.syncdataType" label="tb">单表</el-radio>
          <el-radio v-model="form.syncdataType" label="db">整库</el-radio>
        </el-form-item>
        <el-form-item label="数据信息" prop="syncdata">
          <div v-if="form.sourcetypes == '1'">
            <el-input v-model="form.syncdata" placeholder="请输入文件路径（hdfs://xxx:8020/xxx）"/>
          </div>
          <div v-if="form.sourcetypes == '2' || form.sourcetypes == '4'">
            <el-input v-model="form.syncdata" :placeholder="form.syncdataType == 'tb' ? '请输入库表，例database.table' : '请输入库名称，例database'"/>
          </div>
          <div v-else-if="form.sourcetypes == '3'">
            <el-input v-model="form.syncdata" placeholder="请输入topic"/>
          </div>
        </el-form-item>
        <el-form-item label="数据类型" prop="other1" v-if="(form.sourcetypes == '3')">
          <el-radio v-model="form.other1" label="string">string</el-radio>
          <el-radio v-model="form.other1" label="json">json</el-radio>
        </el-form-item>
        <el-form-item label="分隔字符" prop="other2" v-if="form.sourcetypes == '1' || (form.sourcetypes == '3' && form.other1 == 'string')">
          <el-input v-model="form.other2" placeholder="请输入分隔字符"/>
          <div style="font-size: 12px;color: rgb(153, 153, 153);">确认实际数据中包含该分隔字符，若不包含请修改分隔符</div>
        </el-form-item>
        <el-form-item label="nameNode主节点" prop="other2" v-if="form.sourcetypes == '2' ">
          <el-input v-model="form.other2" placeholder="请输入nameNode主节点"/>
        </el-form-item>
        <el-form-item label="目标catalog" prop="distcatalogid">
          <el-select v-model="currentCatalogForm" value-key="id" @change="getDatabaseListForm" placeholder="请选择目catalog" style="width: 100%">
            <el-option v-for="(item,index) in catalogData" :key="index" :label="item.id" :value="item">
              <div>
                <el-tag size="small" :type="item.types=='hadoop'?'success':''">{{ item.types}}</el-tag>
                <span style="margin-left:10px">{{item.id}}</span>
                <span style="margin-left:10px">({{item.descs?item.descs:item.id}})</span>
              </div>
            </el-option>
          </el-select>
        </el-form-item>
        <el-form-item label="目标库" prop="distdatabase">
          <el-select v-model="form.distdatabase" placeholder="请选择目标库" style="width: 100%">
            <el-option v-for="(item,index) in databaseDataForm" :key="index" :label="item" :value="item"></el-option>
          </el-select>
        </el-form-item>
        <el-form-item label="目标表" prop="disttable" v-if="form.syncdataType != 'db' || !isAdd ">
          <el-input v-model="form.disttable" placeholder="请输入目标表" />
        </el-form-item>
      </el-form>
      <div slot="footer" class="dialog-footer">
        <el-button :loading="formLoading" @click="cancel">取 消</el-button>
        <el-button :loading="formLoading" type="primary" @click="submitForm">确 定</el-button>
      </div>
    </el-dialog>
  </div>
</template>

<script>
import { catalogList } from "@/api/catalog";
import { syncTaskList, syncTaskAdd, syncTaskModify, stopKafkaTask, batchAddHiveTask, batchAddMySqlTask } from '@/api/enterLakeSyncTask'
import { databaseList } from '@/api/dbAndTable.js'

export default {
  name: "enterLakeSyncTask",
  data() {
    return {
      // 遮罩层
      loading: true,
      // 选中数组
      ids: [],
      // 非单个禁用
      single: true,
      // 非多个禁用
      multiple: true,
      // 显示搜索条件
      showSearch: true,
      // 总条数
      total: 0,
      // 表格数据
      syncTaskData: [],
      statusObj:{
        "0":{name:'运行中',type:'primary'},
        "1":{name:'失败',type:'danger'},
        "2":{name:'成功',type:'success'},
        "3":{name:'停止',type:'info'},
      },
      // 弹出层类型
      isAdd:'',
      // 是否显示弹出层
      open: false,
      // 查询参数
      queryParams: {
        pageNum: 1,
        pageSize: 10,
      },
      catalogData: [],
      currentCatalog:{},
      databaseData:[],
      // 排序参数
      queryOrder:{},
      // 表单参数
      form: {},
      currentCatalogForm:{},
      databaseDataForm:[],
      // 表单校验
      rules: {
        sourcetypes: [
          { required: true, message: "来源种类不能为空", trigger: "change" }
        ],
        sourceip: [
          { required: true, message: "连接ip不能为空", trigger: "blur" }
        ],
        syncdata: [
          { required: true, message: "数据信息不能为空", trigger: "blur" }
        ],
        other2: [{ required: true, message: "分隔字符不能为空", trigger: "blur" }],
        distcatalogid: [
          { required: true, message: "目标catalog不能为空", trigger: "change" }
        ],
        distdatabase: [
          { required: true, message: "目标库不能为空", trigger: "change" }
        ],
        disttable: [
          { required: true, message: "目标表不能为空", trigger: "change" }
        ],
      },
      formLoading:false,
    };
  },
  created() {
    this.getCatalogList();
    this.syncTaskList();
  },
  methods: {
    /** 查询catalog列表 */
    getCatalogList() {
      catalogList({pageNum: 1,pageSize: 1000,}).then(response => {
        this.catalogData = response.data.list?.filter(item=>item.types!='file')
      });
    },
    /** 查询库 */
    getDatabaseList() {
      this.$set(this.queryParams,'distdatabase','')
      if(this.currentCatalog && this.currentCatalog.id) {
        this.$set(this.queryParams,'distcatalogid',this.currentCatalog.id)
        databaseList({pageNum: 1,pageSize: 1000,...this.currentCatalog}).then(response => {
          this.databaseData = response.data;
        });
      }
    },
    /** 查询库--表单 */
    getDatabaseListForm() {
      this.$set(this.form,'distdatabase','')
      if(this.currentCatalogForm && this.currentCatalogForm.id) {
        this.$set(this.form,'distcatalogid',this.currentCatalogForm.id)
        databaseList({pageNum: 1,pageSize: 1000,...this.currentCatalogForm}).then(response => {
          this.databaseDataForm = response.data;
        });
      }
    },
    // 查询入户任务列表
    syncTaskList() {
      syncTaskList({...this.queryParams,...this.queryOrder}).then(response => {
        this.syncTaskData = response.data.list;
        this.total = response.data.total;
        this.loading = false;
      });
    },
    // 表单动态校验规则
    handleChangeType() {
      this.rules.other2 = []
      if(this.form.sourcetypes == '1' || this.form.sourcetypes == '3'  ) {
        this.$set(this.form,'other1','string')
        this.$set(this.form,'other2',',')
        this.rules.other2 = [{ required: true, message: "分隔字符不能为空", trigger: "blur" }]
      }else if(this.form.sourcetypes == '4'){
        this.$set(this.form,'other2','3306')
      }else {
        this.$set(this.form,'syncdataType','tb')
        var validNameNode = (rule, value, callback) => {
          var ipRules = /^(\d{1,2}|1\d\d|2[0-4]\d|25[0-5])\.(\d{1,2}|1\d\d|2[0-4]\d|25[0-5])\.(\d{1,2}|1\d\d|2[0-4]\d|25[0-5])\.(\d{1,2}|1\d\d|2[0-4]\d|25[0-5])$/
          if(!value.match(ipRules)) {
            callback(new Error('nameNode主节点IP格式不正确，请重新输入'))
          }else{
            callback();
          }
        };
        this.$set(this.form,'other2','')
        this.rules.other2 = [
          { required: true, message: "nameNode主节点不能为空", trigger: "blur" },
          { validator: validNameNode, trigger: 'blur' }
        ]
      }
    },
    // 排序
    handleSortChange(data) {
      this.queryOrder.orderByColumn = data.prop
      this.queryOrder.isAsc = data.order=="descending"?'desc':'asc'
      this.syncTaskList()
    },
    // 取消按钮
    cancel() {
     this.open = false;
    },
    /** 搜索按钮操作 */
    handleQuery() {
      this.queryParams.pageNum = 1;
      this.syncTaskList();
    },
    /** 重置按钮操作 */
    resetQuery() {
      this.queryParams = {
        pageNum: 1,
        pageSize: 10,
      },
      this.handleQuery();
    },

    /** 新增按钮操作 */
    handleAdd() {
      this.isAdd = true;
      this.form = {sourcetypes:'1',other2:','}
      this.currentCatalogForm = null
      this.open = true;
    },
    /** 修改按钮操作 */
    handleUpdate(row) {
      this.isAdd = false;
      this.form = JSON.parse(JSON.stringify(row))
      this.currentCatalogForm = this.catalogData.filter(item=>item.id == this.form.distcatalogid)[0]
      this.open = true;
    },
    // 停止任务
    handleStop(row) {
      this.$modal.confirm('是否确认停止该任务？').then(function() {
        return stopKafkaTask({id:row.id});
      }).then(() => {
        this.syncTaskList();
        this.$modal.msgSuccess("操作成功");
      }).catch(() => {});
    },
    /** 提交按钮 */
    submitForm: function() {
      this.$refs["form"].validate(valid => {
        if (valid) {
          this.formLoading = true
          if (this.isAdd) {
            if(this.form.sourcetypes == '2' && this.form.syncdataType == 'db') {
              batchAddHiveTask(this.form).then(response => {
                // this.$modal.msgSuccess("新增成功");
                // this.open = false;
                // this.syncTaskList();
              }).finally(()=>{
                this.formLoading = false
              })
            } else if(this.form.sourcetypes == '4' && this.form.syncdataType == 'db') {
              batchAddMySqlTask(this.form).then(response => {
                // this.$modal.msgSuccess("新增成功");
                // this.open = false;
                // this.syncTaskList();
              }).finally(()=>{
                this.formLoading = false
              })
            } else {
              syncTaskAdd(this.form).then(response => {
                // this.$modal.msgSuccess("新增成功");
                // this.open = false;
                // this.syncTaskList();
              }).finally(()=>{
                this.formLoading = false
              })
            }
          } else {
            syncTaskModify(this.form).then(response => {
              // this.$modal.msgSuccess("修改成功");
              // this.open = false;
              // this.syncTaskList();
            }).finally(()=>{
              this.formLoading = false
            })
          }
          this.$modal.msgSuccess("操作成功");
          this.formLoading = false
          this.open = false;
          this.syncTaskList();

        }
      });
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
  .settingItem {
    width: 100%;
    display: flex;
    align-items: center;
    justify-content: space-between;
    margin-bottom: 10px;
    padding: 0 10px;
    border-radius: 4px;
  }
  .success {
    background-color: #f0f9eb;
    border-color: #e1f3d8;
    color: #67c23a;
  }
  .info {
    background-color: #f4f4f5;
    border-color: #e9e9eb;
    color: #909399;
  }
</style>
