<template>
  <div class="app-container">
    <el-form :model="queryParams" ref="queryForm" size="small" :inline="true" v-show="showSearch">
      <el-form-item label="catalog名称" prop="id">
        <el-input
          v-model="queryParams.id"
          placeholder="请输入catalog名称"
          clearable
          style="width: 240px"
          @keyup.enter.native="handleQuery"
        />
      </el-form-item>
      <el-form-item label="catalog类型" prop="types">
        <el-select
          v-model="queryParams.types"
          placeholder="请选择catalog类型"
          clearable
          style="width: 240px"
        >
          <el-option label="hive" value="hive"/>
          <el-option label="hadoop" value="hadoop"/>
          <el-option label="file" value="file"/>
        </el-select>
      </el-form-item>
      <el-form-item label="hive元数据地址" prop="hiveurl">
        <el-input
          v-model="queryParams.hiveurl"
          placeholder="请输入hive元数据地址(thrift://xxx:9083)"
          clearable
          style="width: 240px"
          @keyup.enter.native="handleQuery"
        />
      </el-form-item>
      <el-form-item label="hdfs存储路径" prop="hdfsurl">
        <el-input
          v-model="queryParams.hdfsurl"
          placeholder="请输入hdfs存储路径(hdfs://xxx:8020/xxx/xxx)"
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
      <!-- <el-col :span="1.5">
        <el-button
          type="danger"
          plain
          icon="el-icon-delete"
          size="mini"
          :disabled="multiple"
          @click="handleDelete"
          v-hasPermi="['system:role:remove']"
        >删除</el-button>
      </el-col> -->
      <right-toolbar :showSearch.sync="showSearch" @queryTable="getCatalogList"></right-toolbar>
    </el-row>

    <el-table v-loading="loading" :data="catalogData" @selection-change="handleSelectionChange" @sort-change="handleSortChange">
      <el-table-column type="selection" width="55" align="center" />
      <el-table-column label="catalog名称" prop="id" width="150" sortable/>
      <el-table-column label="catalog类型" prop="types" width="100">
          <template slot-scope="scope">
            <el-tag size="small" :type="scope.row.types=='hadoop'?'success':(scope.row.types=='hive'?'':'warning')">{{ scope.row.types}}</el-tag>
          </template>
        </el-table-column>
      <el-table-column prop="hiveurl" label="hive元数据地址" width="220">
        <template slot-scope="scope">
          <el-tooltip class="item" effect="dark" :content="scope.row.hiveurl" placement="top">
            <div class="tooltipDiv">{{scope.row.hiveurl}}</div>
          </el-tooltip>
        </template>
      </el-table-column>
      <el-table-column prop="hdfsurl" label="hdfs存储路径">
        <template slot-scope="scope">
          <el-tooltip class="item" effect="dark" :content="scope.row.hdfsurl" placement="top">
            <div class="tooltipDiv">{{scope.row.hdfsurl}}</div>
          </el-tooltip>
        </template>
      </el-table-column>
      <el-table-column label="描述" prop="descs" :show-overflow-tooltip="true" width="220" sortable/>
      <el-table-column label="创建人" prop="creataby" width="100"/>
      <el-table-column label="创建时间" prop="createtime" width="180" sortable>
        <template slot-scope="scope">
          <span>{{ parseTime(scope.row.createtime) }}</span>
        </template>
      </el-table-column>
      <el-table-column label="操作" align="right" width="120">
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
      @pagination="getCatalogList"
    />

    <!-- 添加或修改catalog配置对话框 -->
    <el-dialog :title="isAdd?'新增catalog':'修改catalog'" :visible.sync="open" v-if="open" width="500px" :before-close="cancel" append-to-body :close-on-click-modal="false">
      <el-form ref="form" :model="form" :rules="rules" label-width="100px" v-loading="formLoading">
        <el-form-item label="名称" prop="id">
          <el-input v-model="form.id" placeholder="请输入catalog名称" :disabled="!isAdd || settingFilesAlreadyUpload.length>0"/>
        </el-form-item>
        <el-form-item label="catalog类型" prop="types">
          <el-select v-model="form.types" placeholder="请选择catalog类型" @change="handleChangeTypes" style="width:100%">
            <el-option label="hive" value="hive"/>
            <el-option label="hadoop" value="hadoop"/>
            <el-option label="file" value="file"/>
          </el-select>
        </el-form-item>
        <el-form-item :label="form.types == 'file'?'根catalog':'存储路径'" prop="hdfsurl">
          <el-input v-model="form.hdfsurl" :placeholder="form.types == 'file'?'请输入hdfs根catalog':'请输入hdfs存储路径（hdfs://xxx:8020/xxx）'" @blur="handleBlurHdfsUrl"/>
        </el-form-item>
        <el-form-item label="元数据地址" prop="hiveurl" v-if="form.types == 'hive'">
          <el-input v-model="form.hiveurl" placeholder="请输入hive元数据地址 (thrift://xxx:9083)" />
        </el-form-item>
        <el-form-item label="描述">
          <el-input v-model="form.descs" type="textarea" placeholder="请输入配置catalog描述"></el-input>
        </el-form-item>
        <el-form-item label="配置文件" v-if="form.types == 'hive' && form.id" prop="settings">
          <div v-for="(item,index) in settingFiles" :key="index" :class="isAlreadyUpload(item)?'settingItem success':'settingItem info'">
            <div class="settingFile">{{item}}</div>
            <div class="settingOperation">
              <el-upload
                v-if="!isAlreadyUpload(item)"
                action=""
                :multiple="false"
                :show-file-list="false"
                :http-request="handleUpload"
                :before-upload="item=='core-site.xml'?handleBeforeUploadCore:(item=='hdfs-site.xml')?handleBeforeUploadHdfs:handleBeforeUploadHive"
                accept=".xml"
                name="file">
                <el-button title="上传" type="text" icon="el-icon-upload2"></el-button>
              </el-upload>
              <el-button @click="handleDownload(item)" v-if="isAlreadyUpload(item)" type="text" icon="el-icon-download" title="下载"></el-button>
              <el-button @click="handleDeleteFile(item)" v-if="isAlreadyUpload(item)" type="text" icon="el-icon-close" title="删除"></el-button>
            </div>
          </div>
        </el-form-item>
      </el-form>
      <div slot="footer" class="dialog-footer">
        <el-button @click="cancel">取 消</el-button>
        <el-button type="primary" @click="submitForm">确 定</el-button>
      </div>
    </el-dialog>
  </div>
</template>

<script>
import { catalogList, catalogDelete, catalogAdd, catalogModify, getHiveCatalogFiles, upLoadHiveCatalogFile, deleteHiveCatalogFiles,downloadFile } from "@/api/catalog";

export default {
  name: "catalog",
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
      // 角色表格数据
      catalogData: [],
      // 弹出层类型
      isAdd:'',
      // 是否显示弹出层
      open: false,
      // 查询参数
      queryParams: {
        pageNum: 1,
        pageSize: 10,
      },
      // 排序参数
      queryOrder:{},
      // 表单参数
      form: {types:'hadoop'},
      // 表单校验
      rules: {
        id: [
          { required: true, message: "名称不能为空", trigger: "blur" }
        ],
        types: [
          { required: true, message: "catalog类型不能为空", trigger: "change" }
        ],
        hdfsurl: [
          { required: true, message: "hdfs存储路径不能为空", trigger: "blur" }
        ],
        hiveurl:[]
      },
      settingFiles:["core-site.xml","hdfs-site.xml","hive-site.xml"],
      formLoading:false,
      settingFilesAlreadyUpload:[],
      uploadResult:false,
    };
  },
  created() {
    this.getCatalogList();
  },
  methods: {
    handleBlurHdfsUrl() {
      this.rules.hdfsurl[0].message = this.form.types == 'file'?'根catalog不能为空':'存储路径(hdfs://xxx:8020/xxx)不能为空'
    },
    /** 查询catalog列表 */
    getCatalogList() {
      this.loading = true;
      catalogList({...this.queryParams,...this.queryOrder}).then(response => {
        this.catalogData = response.data.list;
        this.total = response.data.total;
        this.loading = false;
      });
    },
    handleSortChange(data) {
      this.queryOrder.orderBy = data.prop
      this.queryOrder.isAsc = data.order=="descending"?'desc':'asc'
      this.getCatalogList()
    },
    // 取消按钮
    cancel() {
      if(this.form.type == 'hive' && this.settingFilesAlreadyUpload.length != 3) {
        this.$message.warning('缺少配置文件未上传')
      } else {
        this.open = false;
        this.reset();
      }

    },
    // 表单重置
    reset() {
      this.form = {types:'hadoop'};
    },
    /** 搜索按钮操作 */
    handleQuery() {
      this.queryParams.pageNum = 1;
      this.getCatalogList();
    },
    /** 重置按钮操作 */
    resetQuery() {
      this.queryParams = {
        pageNum: 1,
        pageSize: 10,
      },
      this.handleQuery();
    },
    // 多选框选中数据
    handleSelectionChange(selection) {
      this.ids = selection.map(item => item.roleId)
      this.single = selection.length!=1
      this.multiple = !selection.length
    },
    /** 新增按钮操作 */
    handleAdd() {
      this.reset();
      this.isAdd = true;
      this.open = true;
      this.settingFilesAlreadyUpload = []
    },
    /** 修改按钮操作 */
    handleUpdate(row) {
      this.isAdd = false;
      this.form = JSON.parse(JSON.stringify(row))
      this.handleChangeTypes()
      this.open = true;
      this.getHiveCatalogFiles()
    },
    handleChangeTypes(){
      if(this.form.types == 'hive') {
        this.rules.hiveurl = [
          { required: true, message: "元数据地址（thrift://xxx:9083）不能为空", trigger: "blur" }
        ]
      }else {
        this.rules.hiveurl = []
      }
      this.rules.hdfsurl[0].message = this.form.types == 'file'?'根catalog不能为空':'hdfs存储路径(hdfs://xxx:8020/xxx)不能为空'
    },
    // 配置文件维护-----start
    // 查询当前配置文件
    getHiveCatalogFiles() {
      this.formLoading = true
      getHiveCatalogFiles(this.form.id).then(res=>{
        if(res.code == 200) {
          this.settingFilesAlreadyUpload = res.data || []
        }
      }).finally(()=>{
        this.formLoading = false
      })
    },
    isAlreadyUpload(file) {
      return this.settingFilesAlreadyUpload.indexOf(file) != -1
    },
    // 上传前判断文件名是否正确
    handleBeforeUploadCore(file) {
      this.handleBeforeUpload(file.name,"core-site.xml")
    },
    handleBeforeUploadHdfs(file) {
      this.handleBeforeUpload(file.name,"hdfs-site.xml")
    },
    handleBeforeUploadHive(file) {
      this.handleBeforeUpload(file.name,"hive-site.xml")
    },
    handleBeforeUpload(fileName,currentFile) {
      console.log(111,fileName,currentFile);
      if(fileName !== currentFile) {
        this.$message.warning('文件名称错误，请重新上传')
        this.uploadResult = false
      }else {
        this.uploadResult = true
      }
    },
    // 上传配置文件
    handleUpload(file) {
      if(this.uploadResult) {
        this.formLoading = true
        let data = new FormData()
        data.append('file',file.file)
        upLoadHiveCatalogFile(this.form.id,data).then(res=>{
          this.$message.success('配置文件上传成功')
          this.getHiveCatalogFiles()
        }).finally(()=>{
          this.formLoading = false
        })
      }
    },
    // 删除配置文件
    handleDeleteFile(file) {
      this.formLoading = true
      deleteHiveCatalogFiles(this.form.id,file.split('-')[0]).then(res=>{
        if(res.code == 200) {
          this.$message.success('删除' + file + '成功，请重新上传！')
          this.getHiveCatalogFiles()
        }
      }).finally(()=>{
        this.formLoading = false
      })
    },
    // 下载配置文件
    handleDownload(item) {
      downloadFile(this.form.id,item.split('-')[0]).then(res=>{
        const blob = new Blob([res])
        const link = document.createElement('a')
        link.style.display = 'none'
        link.href = URL.createObjectURL(blob)
        link.download = item // 下载的文件名
        document.body.appendChild(link)
        link.click()
        document.body.removeChild(link)
      })
    },
    // 配置文件维护-------end
    /** 提交按钮 */
    submitForm: function() {
      this.$refs["form"].validate(valid => {
        if (valid) {
          if (this.isAdd) {
            catalogAdd(this.form).then(response => {
              this.$modal.msgSuccess("新增成功");
              this.open = false;
              this.getCatalogList();
            });
          } else {
            catalogModify(this.form).then(response => {
              this.$modal.msgSuccess("修改成功");
              this.open = false;
              this.getCatalogList();
            });
          }
        }
      });
    },
    /** 删除按钮操作 */
    handleDelete(row) {
      this.$modal.confirm('是否确认删除catalog名称为"' + row.id + '"的数据项？').then(function() {
        return catalogDelete({id:row.id});
      }).then(() => {
        this.getCatalogList();
        this.$modal.msgSuccess("删除成功");
      }).catch(() => {});
    },
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
