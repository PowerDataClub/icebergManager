<template>
  <div class="app-container admin_staff">
    <div style="padding:0 10px" class="admin_staff_box">
      <!--部门数据-->
      <div style="border: 2px solid #eaeaea;padding:0 10px" class="admin_staff_left">
        <div class="head-container header-area">
          <el-select size="small" v-model="currentCatalog" value-key="id" @change="getDatabaseList" style="width:80%;margin-right:8px;">
            <el-option v-for="(item,index) in catalogData" :key="index" :label="item.id" :value="item">
              <div>
                <el-tag size="small" :type="item.types=='hadoop'?'success':''">{{ item.types}}</el-tag>
                <span style="margin-left:10px">{{item.id}}</span>
                <span style="margin-left:10px">({{item.descs?item.descs:item.id}})</span>
              </div>
            </el-option>
          </el-select>
          <el-tooltip class="item" effect="dark" content="新建库" placement="bottom">
            <el-button type="primary" size="small" icon="el-icon-plus" @click="handleAddDb"></el-button>
          </el-tooltip>
        </div>
        <div class="head-container" v-loading="treeLoading">
          <el-tree
            style="height: calc(100vh - 200px);overflow-y: auto;"
            :data="dbAndTableData"
            :props="defaultProps"
            :expand-on-click-node="true"
            ref="tree"
            node-key="label"
            highlight-current
            :load="handleDbClick"
            lazy
            accordion
            @node-click="handleTableClick">
            <span class="custom-tree-node" slot-scope="{ node, data }">
              <div v-if="data.type == 'search' && showSearch" style="width:100%;margin-left:-6px">
                <el-input placeholder="请输入表名进行筛选" size="mini" v-model="queryForm.tableName">
                  <el-button slot="append" icon="el-icon-search" @click.native="queryForm.pageNum=1;handleDbClick(node,'search')"></el-button>
                </el-input>
              </div>
              <div v-if="data.type == 'pagination'" style="width:100%;text-align:right">
                <el-pagination
                  small
                  @size-change="handleSizeChange($event,node)"
                  @current-change="handleCurrentChange($event,node)"
                  :current-page="queryForm.pageNum"
                  :page-sizes="[100, 200, 300, 400]"
                  :page-size="queryForm.pageSize"
                  layout="total, prev, pager, next"
                  :total="total[data.pLabel]"
                  :pager-count="3">
                </el-pagination>
              </div>
              <div class="custom-tree-node" v-if="data.type != 'search' && data.type != 'pagination'">
                <span style="display:flex;width: calc(100% - 62px);align-items: center;">
                  <svg-icon :icon-class="data.type == 'database'?'database':'tables'" style="margin-right:4px"/>
                  <div class="treeLabel" :title="node.label">{{ node.label }}</div>
                </span>
                <span v-if="data.type == 'database'">
                  <el-button
                    title="查询"
                    type="text"
                    size="mini"
                    @click.stop="() => handleSearch(node, data)">
                    <i class="el-icon-search"></i>
                  </el-button>
                  <el-button
                    title="新建表"
                    type="text"
                    size="mini"
                    @click.stop="() => append(node, data)">
                    <i class="el-icon-plus"></i>
                  </el-button>
                  <el-button
                    title="删除库"
                    type="text"
                    size="mini"
                    @click.stop="() => remove(node, data)">
                    <i class="el-icon-delete"></i>
                  </el-button>
                </span>
                <span v-else>
                  <!-- <el-button
                    title="表复制"
                    type="text"
                    size="mini"
                    @click.stop="() => copyTable(node, data)">
                    <i class="el-icon-document-copy"></i>
                  </el-button> -->
                  <el-button
                    title="编辑表"
                    type="text"
                    size="mini"
                    @click.stop="() => editTable(node, data)">
                    <i class="el-icon-edit"></i>
                  </el-button>
                  <el-button
                    title="删除表"
                    type="text"
                    size="mini"
                    @click.stop="() => remove(node, data)">
                    <i class="el-icon-delete"></i>
                  </el-button>
                  <el-dropdown szie="medium" style="margin-left:10px">
                    <span class="el-dropdown-link">
                      <i class="el-icon-more"></i>
                    </span>
                    <el-dropdown-menu slot="dropdown">
                      <el-dropdown-item @click.native="() => copyTable(node, data)">表复制</el-dropdown-item>
                      <el-dropdown-item @click.native="() => clearTable(node, data)">表清空</el-dropdown-item>
                      <el-dropdown-item @click.native="() => handleOpenUnloading(node, data)">转存到hdfs</el-dropdown-item>
                      <el-dropdown-item >
                        <el-upload
                          action=""
                          :multiple="false"
                          :show-file-list="false"
                          :http-request="(file)=>handleUpload(file,data)"
                          accept=".xls,.xlsx"
                          name="file">
                          导入
                        </el-upload>
                      </el-dropdown-item>
                      <el-dropdown-item @click.native="() => handleMergeSmallFile(node, data)">小文件合并</el-dropdown-item>
                    </el-dropdown-menu>
                  </el-dropdown>
                </span>
              </div>

            </span>
          </el-tree>
        </div>


      </div>
      <div class="resize" title="拖拽">
          <i class="el-icon-s-operation" style="font-size:12px"></i>
        </div>
      <!--表数据-->
      <div class="admin_staff_right">
        <!-- 拖拽事件遮罩 -->
        <div class="iframeCover" v-if="isDrag"></div>
        <div class="emptyArea" v-if="!currentTable.label">
          <img src="../../assets/images/empty.jpg" alt="">
          <div style="color:#999">请先选择表</div>
        </div>
        <div v-else>
          <h3 style="margin-left:10px;margin-top:2px">{{currentTable.label}}</h3>
        </div>
        <div v-if="currentTable.label" :key="isChange">
          <div class="metrics-area">
            <div class="metrics-item">
              <span class="title">表大小(KB)</span>
              <span class="content">{{metricsData.tableSize}}</span>
            </div>
            <div class="metrics-item">
              <span class="title">文件数量</span>
              <span class="content">{{metricsData.files}}</span>
            </div>
            <div class="metrics-item">
              <span class="title">平均文件大小(KB)</span>
              <span class="content">{{metricsData.avgFileSize}}</span>
            </div>
            <div class="metrics-item">
              <span class="title">数据行数</span>
              <span class="content">{{metricsData.records}}</span>
            </div>
            <div class="metrics-item">
              <span class="title">最近提交时间</span>
              <span class="content">{{parseTime(metricsData.lastCommitTime) || '-' }}</span>
            </div>
          </div>
          <div style="position:relative">
            <!-- <el-button :loading="mergeLoading" style="position: absolute;right: 0px;top: 4px;z-index:1" type="primary" size="small" @click="handleMergeSmallFile">小文件合并 </el-button> -->
            <el-tabs v-model="activeName" style="margin-left:10px" :key="isChange">
              <el-tab-pane label="表结构" name="first">
                <ColumnsAndPartition v-if="activeName=='first'" :currentCatalog="currentCatalog" :currentTable="currentTable"/>
              </el-tab-pane>
              <el-tab-pane label="快照信息" name="second">
                <Snapshot v-if="activeName=='second'" :currentCatalog="currentCatalog" :currentTable="currentTable"/>
              </el-tab-pane>
              <el-tab-pane label="数据预览" name="three">
                <TableData v-if="activeName=='three'" :currentCatalog="currentCatalog" :currentTable="currentTable" :catalogList="catalogFileList" @dataChange="handleDataChange"/>
              </el-tab-pane>
              <el-tab-pane label="元数据信息" name="four">
                <MetaDataInfo v-if="activeName=='four'" :currentCatalog="currentCatalog" :currentTable="currentTable"/>
              </el-tab-pane>
            </el-tabs>
          </div>
        </div>
      </div>
    </div>

    <!-- 添加库配置对话框 -->
    <el-dialog title="新建库" :visible.sync="addDbVisible" v-if="addDbVisible" width="600px" append-to-body>
      <el-form ref="dbForm" :model="dbForm" :rules="dbRules" label-width="80px">
        <el-form-item label="库名称" prop="databaseName">
          <el-input v-model="dbForm.databaseName" placeholder="请输入库名称" />
        </el-form-item>
      </el-form>
      <div slot="footer" class="dialog-footer">
        <el-button type="primary" @click="submitDbForm">确 定</el-button>
        <el-button @click="dbForm={},addDbVisible=false">取 消</el-button>
      </div>
    </el-dialog>

    <!-- 添加表配置对话框 -->
    <el-dialog :title="isAdd?'新建表':'编辑表'" :visible.sync="editTableVisible" v-if="editTableVisible" width="70%" append-to-body>
      <el-form ref="tableForm" :model="tableForm" :rules="tableRules" label-width="80px">
        <el-form-item label="表名称" prop="tableName">
          <el-input v-model="tableForm.tableName" placeholder="请输入表名称" :disabled="!isAdd"/>
        </el-form-item>
      </el-form>
      <ColumnTable ref="ColumnTable" :tableInfo="tableInfo" :isHasSnapshot="isHasSnapshot"/>
      <div slot="footer" class="dialog-footer">
        <el-button :loading="isBtnLoading" type="primary" @click="submitTableForm">确 定</el-button>
        <el-button :loading="isBtnLoading" @click="editTableVisible=false">取 消</el-button>
      </div>
    </el-dialog>


    <!-- 表复制 -->
    <el-dialog title="表复制" :visible.sync="copyTableVisible" v-if="copyTableVisible" width="40%" append-to-body :before-close="()=>{copyTableForm={},copyTableVisible=false}">
      <el-form ref="copyTableForm" :model="copyTableForm" :rules="copyTableRules" label-width="120px">
        <el-form-item label="表名称" prop="tableName">
          <el-input v-model="copyTableForm.tableName" placeholder="请输入表名称" :disabled="!isAdd"/>
        </el-form-item>
        <el-form-item label="catalog" prop="currentCatalog">
          <el-select size="small" v-model="copyTableForm.currentCatalog" value-key="id" @change="getDatabaseListChoose">
            <el-option v-for="(item,index) in catalogData" :key="index" :label="item.id" :value="item">
              <div>
                <el-tag size="small" :type="item.types=='hadoop'?'success':''">{{ item.types}}</el-tag>
                <span style="margin-left:10px">{{item.id}}</span>
                <span style="margin-left:10px">({{item.descs?item.descs:item.id}})</span>
              </div>
            </el-option>
          </el-select>
        </el-form-item>
        <el-form-item label="库" prop="databaseName">
          <el-select size="small" v-model="copyTableForm.databaseName">
            <el-option v-for="(item,index) in dbAndTableDataChoose" :key="index" :label="item.label" :value="item.label"></el-option>
          </el-select>
        </el-form-item>
        <el-form-item label="复制数据" prop="isData">
          <el-radio-group v-model="copyTableForm.isData">
            <el-radio :label="true">是</el-radio>
            <el-radio :label="false">否</el-radio>
          </el-radio-group>
        </el-form-item>
      </el-form>
      <div slot="footer" class="dialog-footer">
        <el-button :loading="isBtnLoading" type="primary" @click="submitCopyTableForm">确 定</el-button>
        <el-button :loading="isBtnLoading" @click="copyTableForm={},copyTableVisible=false">取 消</el-button>
      </div>
    </el-dialog>

    <!-- 转存到hdfs -->
    <el-dialog title="数据转存到hdfs" :visible.sync="unloadingVisible" v-if="unloadingVisible" width="600px" append-to-body>
      <el-form ref="unloadingForm" :model="unloadingForm" :rules="unloadingFormRules" label-width="120px">
        <el-form-item label="catalog名称" prop="catalog">
          <el-select v-model="unloadingForm.catalog" placeholder="请选择catalog名称" style="width:100%" value-key="id">
            <el-option v-for="(item,index) in catalogFileList" :key="index" :label="item.id" :value="item">
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
import { catalogList } from "@/api/catalog";
import { databaseList,
  tableList,
  databaseAdd,
  databaseDelete,
  tableDelete,
  tableMetrics,
  tableCreate,
  queryTableInfo,
  tableSnapshot,
  updateTable,
  mergeSmallFile,
  copyTable,
  clearTableData,
  importTableData,
  tableDataToFile
} from '@/api/dbAndTable.js'
import ColumnsAndPartition from './components/ColumnsAndPartition.vue'
import Snapshot from './components/Snapshot.vue'
import TableData from './components/TableData.vue'
import MetaDataInfo from './components/MetaDataInfo.vue'

import ColumnTable from './components/addTable/ColumnTable.vue'

export default {
  name: "dbAndTable",
  components: { ColumnsAndPartition, Snapshot, TableData, ColumnTable, MetaDataInfo },
  data() {
    return {
      // 遮罩层
      treeLoading:false,
      loading: true,
      // catalog数据
      currentCatalog:{},
      catalogData:[],
      catalogFileList:[],
      // 树
      defaultProps:{
        label: 'label',
        children: 'children',
        isLeaf: 'leaf'
      },
      dbAndTableData:[],
      // 当前选中表
      currentTable:{},
      // 库
      addDbVisible:false,
      dbForm:{},
      dbRules: {
        databaseName: [
          { required: true, message: "库名称不能为空", trigger: "blur" }
        ]
      },
      // 表
      queryForm:{
        tableName:'',
        pageNum:1,
        pageSize:100,
      },
      total:{},
      showSearch:false,
      isBtnLoading:false,
      isChange:0,
      metricsData:{},
      activeName:'first',
      currentNode:{},
      editTableVisible:false,
      isAdd:true,
      tableForm:{},
      tableRules: {
        tableName: [
          { required: true, message: "表名称不能为空", trigger: "blur" }
        ]
      },
      isHasSnapshot:false,
      tableInfo:{},
      mergeLoading:false,
      currentTableList:[],
      copyTableVisible:false,
      copyTableForm:{},
      currentCopyTable:{},
      copyTableRules: {
        tableName: [
          { required: true, message: "表名称不能为空", trigger: "blur" }
        ],
        currentCatalog: [
          { required: true, message: "catalog不能为空", trigger: "change" }
        ],
        databaseName: [
          { required: true, message: "库不能为空", trigger: "change" }
        ]
      },
      dbAndTableDataChoose:[],
      afterDelNode:null,

      // 拖拽遮罩
      isDrag:false,

      // 转存到hdfs
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
    };
  },
  created() {
    this.getCatalogList()
    this.$nextTick(()=>{
      this.dragControllerDiv()
    })
  },
  methods: {
    // 拖拽方法
    dragControllerDiv() {
        var resize = document.getElementsByClassName('resize')[0]
        var bodyArea = document.getElementsByClassName('admin_staff')[0]
        var leftBody = document.getElementsByClassName('admin_staff_left')[0]
        var rightBody = document.getElementsByClassName('admin_staff_right')[0]
        resize.onmousedown = (e) => {
            this.isDrag = true
            resize.style.background = "#999"
            resize.style.color = "#fff"
            resize.left = resize.offsetLeft
            var startX = e.clientX
            document.onmousemove = (event) => {
                var endX = event.clientX
                var movelen = resize.left + (endX - startX)
                // 左侧保留最小宽度
                const maxT = bodyArea.clientWidth - resize.offsetWidth
                if(movelen < 320) movelen = 320
                if(movelen > maxT - 320) movelen = maxT - 320
                leftBody.style.width = movelen + 'px'
                rightBody.style.width = `calc(100% - ${movelen}px)`
            }
            document.onmouseup = (event) => {
            document.onmousemove = null
            document.onmouseup = null
            resize.style.background = "#d6d6d6"
            resize.style.color = "#999"
            resize.releaseCapture && resize.releaseCapture()
                this.isDrag = false
            }
        }
    },
    // 刷新
    handleDataChange() {
      this.getTableMetrics()
    },
    /** 查询Catalog列表 */
    getCatalogList() {
      this.loading = true;
      catalogList({pageNum:1,pageSize:1000}).then(response => {
        this.catalogData = response.data.list?.filter(item=>item.types!='file');
        this.catalogFileList = response.data.list?.filter(item=>item.types=='file');
        this.currentCatalog = this.catalogData[0]
        this.getDatabaseList()
      })
    },

    /** 查询库 */
    getDatabaseList() {
      this.treeLoading = true
      databaseList({...this.currentCatalog}).then(res=>{
        this.dbAndTableData = res.data?res.data.map(item=>{
          return {
            id:item,
            label:item,
            type:'database',
            children:[]
          }
        }):[]
        this.loading = false;
      }).catch(err=>{
        this.dbAndTableData = []
        this.currentTable = ''
      }).finally(()=>{
        this.treeLoading = false
      })
    },
    getDatabaseListChoose() {
      this.$set( this.copyTableForm,'databaseName','')
      databaseList({...this.copyTableForm.currentCatalog}).then(res=>{
        this.dbAndTableDataChoose = res.data?res.data.map(item=>{
          return {
            id:item,
            label:item,
            type:'database',
            children:[]
          }
        }):[]
      }).catch(err=>{
        this.dbAndTableDataChoose = []
      })
    },

    /** 新建库 */
    handleAddDb() {
      this.dbForm={}
      this.addDbVisible = true
    },
    submitDbForm() {
      this.$refs['dbForm'].validate((valid) => {
        if (valid) {
          databaseAdd({
            ...this.currentCatalog,
            ...this.dbForm
          }).then(res=>{
            this.$modal.msgSuccess("新增成功");
            this.addDbVisible = false;
            this.getDatabaseList()
          })
        } else {
          console.log('error submit!!');
          return false;
        }
      })
    },

    /** 新建表 */
    append(node,data) {
      this.currentNode = this.afterDelNode?this.afterDelNode:node
      this.isAdd = true
      this.tableForm = {}
      this.tableInfo = {}
      this.editTableVisible = true
    },
    submitTableForm() {
      var isValidate = this.$refs.ColumnTable.validateField()
      if(isValidate) {
        var columnData = JSON.parse(JSON.stringify(this.$refs.ColumnTable.tableForm.columnData))
        columnData.map(item=>{
          item.isNullable = item.isNullable?'1':'0'
          item.isWithZone = item.isWithZone?'1':'0'
          return item
        })
        var columnDataOld = JSON.parse(JSON.stringify(this.$refs.ColumnTable.oldColumnData))
        columnDataOld.map(item=>{
          item.isNullable = item.isNullable?'1':'0'
          item.isWithZone = item.isWithZone?'1':'0'
          return item
        })
      }
      var isValidateP = this.$refs.ColumnTable.validateFieldP()
      if(isValidateP) {
        var partitionData = JSON.parse(JSON.stringify(this.$refs.ColumnTable.tableFormP.partitionData))
        partitionData.map(item=>{
          item.sourceName = item.sourceItem.columnName
          return item
        })
        var oldPartitionData = JSON.parse(JSON.stringify(this.$refs.ColumnTable.oldPartitionData))
        oldPartitionData.map(item=>{
          item.sourceName = item.sourceItem.columnName
          return item
        })
      }
      this.$refs['tableForm'].validate((valid) => {
          if (valid && isValidate && isValidateP) {
            var data = {
              ...this.currentCatalog,
              databaseName:this.isAdd?this.currentNode.data.label:this.currentTable.pLabel,
              ...this.tableForm,
              columnDtos:columnData,
              partitionParams:partitionData
            }
            if(this.isAdd) {
              this.isBtnLoading = true
              tableCreate({...data}).then(res=>{
                this.$modal.msgSuccess("新建成功");
                this.editTableVisible = false
                this.currentNode.data.children.push({
                  id:this.tableForm.tableName,
                  label:this.tableForm.tableName,
                  pLabel: this.currentNode.data.label,
                  type:'table',
                  leaf: true
                });
              }).finally(()=>{
                this.isBtnLoading = false
              })
            }else {
              this.isBtnLoading = true
              var editData
              // if(this.isHasSnapshot) {
                // 字段信息
                var tempData = []
                columnDataOld.map(item=>{
                  if(item.exectype == '3') {
                    tempData.push(item)
                  }
                })
                data.columnDtos.map(item=>{
                  if(item.exectype == '1') {
                    tempData.push(item)
                  }else {
                    var keys = ['columnName','dataType','isNullable','precision','scale','isWithZone','keyType','valueType','comment']
                    var tempOld = columnDataOld.filter(it=>it.index == item.index)[0]
                    var obj = JSON.parse(JSON.stringify(tempOld))
                    keys.forEach(key=>{
                      if(item[key] != tempOld[key]) {
                        obj['new' + key.replace(key[0],key[0].toUpperCase())] = item[key]
                        obj['exectype'] = '2'
                      }
                    })
                    tempData.push(obj)
                  }
                })

                // 分区信息
                var tempDataP = []
                oldPartitionData.map(item=>{
                  if(item.exectype == '3') {
                    tempDataP.push(item)
                  }
                })
                data.partitionParams.map(item=>{
                  if(item.exectype == '1') {
                    tempDataP.push(item)
                  }else {
                    var keys = ['sourceName','type','targetName']
                    var tempOld = oldPartitionData.filter(it=>it.index == item.index)[0]
                    var obj = JSON.parse(JSON.stringify(tempOld))
                    keys.forEach(key=>{
                      if(item[key] != tempOld[key]) {
                        obj['new' + key.replace(key[0],key[0].toUpperCase())] = item[key]
                        obj['exectype'] = '2'
                      }
                    })
                    tempDataP.push(obj)
                  }
                })

                editData = {
                  ...data,
                  columnDtos:tempData.map(item=>{
                    item.exectype = item.exectype || '0'
                    return item
                  }),
                  partitionParams:tempDataP.map(item=>{
                    item.exectype = item.exectype || '0'
                    return item
                  }),
                }
              // }else {
              //   editData = data
              // }
              console.log('result',editData);
              updateTable({...editData}).then(res=>{
                this.$modal.msgSuccess("编辑成功");
                this.editTableVisible = false
                this.isChange++
              }).finally(()=>{
                this.isBtnLoading = false
              })
            }
          } else {
            return false;
          }
        });
    },
    // 表复制
    copyTable(node,data) {
      this.currentCopyTable = data
      this.$set(this.copyTableForm,'tableName',data.label)
      this.$set(this.copyTableForm,'isData',false)
      this.copyTableVisible = true
    },
    submitCopyTableForm() {
      this.$refs['copyTableForm'].validate((valid) => {
        if (valid) {
          var data = {
            dstTable:{
              ...this.copyTableForm.currentCatalog,
              databaseName:this.copyTableForm.databaseName,
              tableName:this.copyTableForm.tableName,
            },
            sourceTable:{
              ...this.currentCatalog,
              databaseName:this.currentCopyTable.pLabel,
              tableName:this.currentCopyTable.label
            },
            isData:this.copyTableForm.isData
          }
          console.log(data);
          this.isBtnLoading = true
          copyTable({...data}).then(res=>{
            if(res.code == 200) {
              this.$message.success('表复制成功')
              if(this.copyTableForm.databaseName == this.currentCopyTable.pLabel) {
                this.$refs.tree.append({
                  id:this.copyTableForm.tableName,
                  label:this.copyTableForm.tableName,
                  pLabel: this.copyTableForm.databaseName,
                  type:'table',
                  leaf: true
                },this.currentCopyTable.pLabel)
              }else {
                this.getDatabaseList()
              }
            }
          }).finally(()=>{
            this.isBtnLoading = false
            this.copyTableVisible = false
          })
        } else {
          return false;
        }
      });
    },
    // 编辑表
    editTable(node,data) {
      tableSnapshot({
        ...this.currentCatalog,
        databaseName:data.pLabel,
        tableName:data.label
      }).then(res=>{
        this.isHasSnapshot = res.data.length>0
      })
      queryTableInfo({
        ...this.currentCatalog,
        databaseName:data.pLabel,
        tableName:data.label
      }).then(res=>{
        if(res.code == 200) {
          this.currentTable = data
          this.tableInfo = res.data
          this.tableForm.tableName = res.data.tableName
          this.isAdd = false
          this.editTableVisible = true
        }
      })
    },
    /** 删除 */
    remove(node,data) {
      if(data.type == 'database') {
        this.$confirm('此操作将永久删除该库以及库下面的表, 是否继续?', '提示', {
          confirmButtonText: '确定',
          cancelButtonText: '取消',
          type: 'warning'
        }).then(() => {
          databaseDelete({
            ...this.currentCatalog,
            databaseName:data.label
          }).then(res=>{
            this.$modal.msgSuccess("删除成功");
            this.getDatabaseList()
          })
        })
      }else {
        this.$confirm('此操作将永久删除该表, 是否继续?', '提示', {
          confirmButtonText: '确定',
          cancelButtonText: '取消',
          type: 'warning'
        }).then(() => {
          tableDelete({
            ...this.currentCatalog,
            databaseName:data.pLabel,
            tableName:data.label
          }).then(res=>{
            this.$modal.msgSuccess("删除成功");
            // 展示层面删除
            const parent = node.parent;
            const children =[];
            parent.childNodes.forEach(item=>{
              children.push(item.data)
            })
            const index = children.findIndex(d => d.label === data.label);
            parent.childNodes.splice(index, 1);
            const indexC = parent.data.children.findIndex(d => d.label === data.label);
            parent.data.children.splice(indexC, 1);
            this.afterDelNode = parent
          })
        })
      }
    },

    // 分页、查询条件
    handleSizeChange(val,node) {
      console.log(val);
      this.queryForm.pageSize = val
      this.handleDbClick(node,'search')
    },
    handleCurrentChange(val,node) {
      this.queryForm.pageNum = val
      this.handleDbClick(node,'search')
    },
    handleSearch(node,data) {
      this.showSearch = !this.showSearch
      this.queryForm.tableName = ''
      var result = JSON.parse(JSON.stringify(this.currentTableList))
      const index = result.findIndex(d => d.label === 'search');
      if(index>-1) {
        result.splice(index, 1);
      }
      if(this.showSearch) {
        result.unshift({
          type :'search',
          label:'search',
          leaf: true
        })
      }
      this.$refs.tree.updateKeyChildren(data.label,result)
    },
    /** 查询表 */
    handleDbClick(node,resolve) {
      var databaseData
      if(resolve == 'search') {
        databaseData = node.parent
      }else {
        this.queryForm.pageNum = 1
        databaseData = node
      }
      if(databaseData.data.type == 'database') {
        this.treeLoading = true
        tableList({
          ...this.currentCatalog,
          databaseName:databaseData.data.label,
          tableName:this.queryForm.tableName,
          pageSize:this.queryForm.pageSize,
          pageNum:this.queryForm.pageNum,
        }).then(res=>{
          var data = res.data.data?res.data.data.map(item=>{
            return {
              id:item,
              label:item,
              pLabel:databaseData.data.label,
              type:'table',
              leaf: true
            }
          }):[]
          if(this.showSearch) {
            data.unshift({
              type :'search',
              label:'search',
              leaf: true
            })
          }
          if(res.data.total>this.queryForm.pageSize) {
            data.push({
              type :'pagination',
              label:'pagination',
              pLabel:databaseData.data.label,
              leaf: true
            })
          }
          this.currentTableList = JSON.parse(JSON.stringify(data))
          if(resolve&&resolve != 'search'&&resolve != 'pagination') {
            this.$nextTick(()=>{
              resolve(data)
            })

          }else {
            this.$refs.tree.updateKeyChildren(databaseData.data.label,data)
          }
          this.total[databaseData.data.label] = res.data.total
          console.log(this.total);
        }).finally(()=>{
          this.treeLoading = false
          this.afterDelNode = null
        })
      }else {
        resolve([])
      }
    },

    /** 查询表详情 */
    handleTableClick(data) {
      if(data.type == 'table') {
        this.currentTable = data
        this.activeName = 'first'
        this.getTableMetrics()
        this.isChange++
      }
    },
    getTableMetrics() {
      tableMetrics({
        ...this.currentCatalog,
        databaseName:this.currentTable.pLabel,
        tableName:this.currentTable.label
      }).then(res=>{
        this.metricsData = res.data
      }).catch(err=>{
        this.metricsData = {}
      })
    },
    // 合并小文件
    handleMergeSmallFile(node,data) {
      this.$confirm('合并小文件会重新初始化快照信息，合并后将无法切换查看其他快照版本数据, 是否继续?', '提示', {
          confirmButtonText: '确定',
          cancelButtonText: '取消',
          type: 'warning'
        }).then(() => {
          this.mergeLoading = true
          mergeSmallFile({
            ...this.currentCatalog,
            databaseName:data?data.pLabel:this.currentTable.pLabel,
            tableName:data?data.label:this.currentTable.label
          }).then(res=>{
            if(res.code == 200) {
              this.$modal.msgSuccess(res.msg);
              this.getTableMetrics()
            }
          }).finally(()=>{
             this.mergeLoading = false
          })
        })
    },
    // 表清空
    clearTable(node,data) {
      this.$confirm('该操作将清空该表数据，是否继续?', '提示', {
          confirmButtonText: '确定',
          cancelButtonText: '取消',
          type: 'warning'
        }).then(() => {
          clearTableData({
            ...this.currentCatalog,
            databaseName:data.pLabel,
            tableName:data.label
          }).then(res=>{
            if(res.code == 200) {
              this.$message.success('表清空数据成功')
              this.getTableMetrics()
              this.isChange++
            }
          })
        })
    },
    // 转存到hdfs到本地
    handleOpenUnloading(node,data) {
      this.currentTable = data
      this.unloadingForm = {splitStr:',',isHead:'0'};
      this.unloadingVisible=true
    },
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
            title: '转存到hdfs结果',
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
    // 导入
    handleUpload(file,nodeData) {
      this.currentTable = nodeData
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
          this.getTableMetrics()
          this.isChange++
        }
      }).finally(()=>{
        this.loading = false
      })
    },
  }
};
</script>
<style lang="scss" scoped>
.header-area {
  display: flex;
  align-items: center;
  margin: 10px 0;
}
.custom-tree-node {
  flex: 1;
  display: flex;
  align-items: center;
  justify-content: space-between;
  font-size: 14px;
  padding-right: 8px;
}
::v-deep .el-tree-node__content {
  height: 32px!important;
  line-height: 32px!important;
}
.metrics-area {
  width: 100%;
  display: flex;
  align-items: center;
  .metrics-item {
    flex:1;
    white-space: nowrap;
    padding: 15px 10px;
    margin: 0 10px 10px 10px;
    box-shadow: 0 0 8px 0 rgba(0,0,0,.1);
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: space-between;

    border: 1px solid rgba(207,215,231,.6);
    // background: linear-gradient(to right ,#cce0fd, #eff3fa );
    // border-radius: 8px;
    .title{
      color: #24324c;
      margin-bottom: 16px;
      font-size: 14px;
    }
    .content {
      color: #4a7bff;
      font-size: 18px;
    }
  }
}
::v-deep.el-input--mini .el-input__inner {
    height: 26px;
    line-height: 26px;
}
.admin_staff_box{
  display: flex;
  width: 100%;
  height: 100%;
  background-color: #fff;
  .admin_staff_left{
      // width: 30%;
      width: 320px;
      // background-color: #fff;
      // box-shadow: 0px 2px 6px 0px rgba(0, 0, 0, 0.1);
      height: 100%;
      // position: relative;
      overflow: auto;
  }
  .resize {
      margin: auto 0 auto -2px;
      width: 12px;
      height:42px;
      background-color: #d9d9d9;
      cursor: col-resize;
      border-radius: 0 4px 4px 0;
      display: flex;
      justify-content: center;
      align-items: center;
      padding: 0 2px;
      color: #999;
  }
  .resize:hover {
      background: #999;
      color: #fff;
  }
  .admin_staff_right{
      flex: auto;
      height: 100%;
      position: relative;
      max-width: calc(100% - 320px);
      .iframeCover {
          position: absolute;
          top: 0;
          bottom: 0;
          left: 0;
          right: 0;
          z-index: 9999999;
          background: transparent;
      }
  }
}
.custom-tree-node{
  width: calc(100% - 24px);
}
.treeLabel {
  width: calc(100% - 20px);
  white-space: normal;
  overflow: hidden;
  text-overflow: ellipsis;
}
.emptyArea {
  width: 100%;
  height: 100%;
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  img {
    width:200px;
    height:200px;
    margin-bottom:20px
  }
}
</style>
