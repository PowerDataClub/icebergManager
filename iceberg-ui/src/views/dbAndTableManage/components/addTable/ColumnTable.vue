<template>
  <div style="max-height:600px;overflow:auto">
    <div class="columnTableTitle">
      <div style="margin-right:10px;font-size:16px;font-weight:bold">字段信息</div>
      <el-button size="mini" plain @click="handleAdd" type="primary" icon="el-icon-plus"></el-button>
      <el-button size="mini" plain @click="handleDel" type="danger" icon="el-icon-delete" :disabled="!selectListToDel.length"></el-button>
    </div>
    <el-form :model="tableForm" :rules="tableFormRules" ref="tableForm" size="mini">
      <el-table
      :data="tableForm.columnData"
      style="width: 100%"
      border
      @selection-change="handleSelectionChange">
        <el-table-column type="selection" width="55" align="center" />
        <el-table-column prop="columnName" label="字段名称" align="center">
          <template slot-scope="scope">
            <el-form-item :prop="'columnData.' + scope.$index + '.columnName'" :rules="tableFormRules.columnName">
              <el-input v-model="scope.row.columnName" placeholder="字段名称"></el-input>
            </el-form-item>
          </template>
        </el-table-column>
        <el-table-column prop="dataType" label="字段类型" align="center">
          <template slot-scope="scope">
            <el-form-item :prop="'columnData.' + scope.$index + '.dataType'" :rules="tableFormRules.dataType">
              <el-select 
                v-if="scope.row.exectype == '1'"
                v-model="scope.row.dataType" 
                placeholder="字段类型" 
                @change="handleChangeType(scope.$index)">
                <el-option
                  v-for="item in valueTypeList"
                  :key="item.value"
                  :label="item.label"
                  :value="item.value">
                </el-option>
                <el-option label="DATE" value="DATE"></el-option>
              </el-select>
              <el-select 
                v-else-if="tableInfo.tableName && isHasSnapshot && scope.row.dataType=='INTEGER'"
                v-model="scope.row.dataType" 
                placeholder="字段类型" 
                @change="handleChangeType(scope.$index)" >
                <el-option label="LONG" value="LONG"></el-option>
              </el-select>
              <el-select 
                v-else-if="tableInfo.tableName && isHasSnapshot && scope.row.dataType=='FLOAT'"
                v-model="scope.row.dataType" 
                placeholder="字段类型" 
                @change="handleChangeType(scope.$index)" >
                <el-option label="DOUBLE" value="DOUBLE"></el-option>
              </el-select>
              <el-select 
                v-else
                v-model="scope.row.dataType" 
                placeholder="字段类型" 
                @change="handleChangeType(scope.$index)" 
                :disabled="tableInfo.tableName && isHasSnapshot && !(scope.row.dataType=='INTEGER' || scope.row.dataType=='FLOAT')">
                <el-option
                  v-for="item in valueTypeList"
                  :key="item.value"
                  :label="item.label"
                  :value="item.value">
                </el-option>
                <el-option label="DATE" value="DATE"></el-option>
              </el-select>
            </el-form-item>
            
          </template>
        </el-table-column>
        <el-table-column prop="isNullable" label="必填" align="center">
          <template slot-scope="scope">
            <el-form-item>
              <el-checkbox v-model="scope.row.isNullable" :disabled="tableInfo.tableName && isHasSnapshot && scope.row.isNullable=='0'"></el-checkbox>
            </el-form-item>
          </template>
        </el-table-column>
        <el-table-column prop="precision" label="总位数" align="center">
          <template slot-scope="scope">
            <el-form-item 
            :prop="'columnData.' + scope.$index + '.precision'" 
            :rules="scope.row.dataType == 'Decimal'?[{required: true, message: '总位数不允许为空', trigger: 'blur'}]:[]">
              <el-input v-model="scope.row.precision" placeholder="总位数" :disabled="scope.row.dataType !== 'Decimal' || isHasSnapshot" type="number" min='0'></el-input>
            </el-form-item>
          </template>
        </el-table-column>
        <el-table-column prop="scale" label="小数位" align="center">
          <template slot-scope="scope">
            <el-form-item 
            :prop="'columnData.' + scope.$index + '.scale'" 
            :rules="scope.row.dataType == 'Decimal'?[{required: true, message: '小数位不允许为空', trigger: 'blur'}]:[]">
              <el-input v-model="scope.row.scale" placeholder="小数位" :disabled="scope.row.dataType !== 'Decimal' || isHasSnapshot" type="number" min="0"></el-input>
            </el-form-item>
          </template>
        </el-table-column>
        <el-table-column prop="isWithZone" label="时区" align="center">
          <template slot-scope="scope">
            <el-form-item>
              <el-checkbox v-model="scope.row.isWithZone" :disabled="scope.row.dataType !== 'Timestamp' || isHasSnapshot"></el-checkbox>
            </el-form-item>
          </template>
        </el-table-column>
        <el-table-column prop="keyType" label="键类型" align="center">
          <template slot-scope="scope">
            <el-form-item 
            :prop="'columnData.' + scope.$index + '.keyType'" 
            :rules="scope.row.dataType == 'Map'?[{required: true, message: '键类型不允许为空', trigger: 'change'}]:[]">
              <el-select v-model="scope.row.keyType" placeholder="键类型" :disabled="scope.row.dataType !== 'Map' || isHasSnapshot">
                <el-option
                  v-for="item in valueTypeList"
                  :key="item.value"
                  :label="item.label"
                  :value="item.value">
                </el-option>
              </el-select>
            </el-form-item>
          </template>
        </el-table-column>
        <el-table-column prop="valueType" label="值类型" align="center">
          <template slot-scope="scope">
            <el-form-item 
            :prop="'columnData.' + scope.$index + '.valueType'" 
            :rules="scope.row.dataType == 'List' || scope.row.dataType == 'Map'?[{required: true, message: '值类型不允许为空', trigger: 'change'}]:[]">
              <el-select v-model="scope.row.valueType" placeholder="值类型" :disabled="!(scope.row.dataType == 'List' || scope.row.dataType == 'Map') || isHasSnapshot">
              <el-option
                v-for="item in valueTypeList"
                :key="item.value"
                :label="item.label"
                :value="item.value">
              </el-option>
            </el-select>
            </el-form-item>
          </template>
        </el-table-column>
        <el-table-column prop="comment" label="字段描述" align="center">
          <template slot-scope="scope">
            <el-form-item>
              <el-input v-model="scope.row.comment" placeholder="字段描述"></el-input>
            </el-form-item>
          </template>
        </el-table-column>
      </el-table>
    </el-form>

    <div class="columnTableTitle" style="margin-top:20px">
      <div style="margin-right:10px;font-size:16px;font-weight:bold">分区信息</div>
      <el-button size="mini" plain @click="handleAddP" type="primary" icon="el-icon-plus"></el-button>
      <el-button size="mini" plain @click="handleDelP" type="danger" icon="el-icon-delete" :disabled="!selectListToDelP.length"></el-button>
    </div>
    <el-form :model="tableFormP" :rules="tableFormRulesP" ref="tableFormP" size="mini">
      <el-table
      :data="tableFormP.partitionData"
      style="width: 100%"
      border
      @selection-change="handleSelectionChangeP">
        <!-- <el-table-column type="selection" width="55" align="center" :selectable="selectable"/> -->
        <el-table-column type="selection" width="55" align="center"/>
        <el-table-column prop="dataType" label="分区字段" align="center">
          <template slot-scope="scope">
            <el-form-item :prop="'partitionData.' + scope.$index + '.sourceItem'" :rules="tableFormRulesP.sourceItem">
              <el-select 
              v-model="scope.row.sourceItem" placeholder="分区字段" 
              value-key="columnName" 
              style="width:100%"
              @change="handleChangeColumn(scope.$index)">
                <el-option
                  v-for="(item,index) in tableForm.columnData"
                  :key="index"
                  :label="item.columnName"
                  :value="item">
                </el-option>
              </el-select>
            </el-form-item>
          </template>
        </el-table-column>
        <el-table-column prop="type" label="类型" align="center">
          <template slot-scope="scope">
            <el-form-item 
            v-if="(scope.row.sourceItem && scope.row.sourceItem.dataType == 'Timestamp') || (!scope.row.sourceItem.dataType && scope.row.dateType == 'Timestamp')"
            :prop="'partitionData.' + scope.$index + '.type'" 
            :rules="[{required: true, message: '类型不允许为空', trigger: 'change'}]">
              <el-select
              v-model="scope.row.type" placeholder="类型"
              style="width:100%">
                <el-option
                  v-for="item in timeType"
                  :key="item.value"
                  :label="item.label"
                  :value="item.value">
                </el-option>
              </el-select>
            </el-form-item>
            <el-form-item 
            v-else
            :prop="'partitionData.' + scope.$index + '.type'">
              <el-select
              v-model="scope.row.type" placeholder="类型"
              style="width:100%">
                <el-option
                  label="identity"
                  value="identity">
                </el-option>
              </el-select>
            </el-form-item>
          </template>
        </el-table-column>
        <el-table-column prop="targetName" label="目标字段" align="center">
          <template slot-scope="scope">
            <el-form-item 
            :prop="'partitionData.' + scope.$index + '.targetName'" 
            :rules="scope.row.sourceItem && scope.row.sourceItem.dataType == 'Timestamp'?[{required: true, message: '类型不允许为空', trigger: 'blur'}]:[]">
              <el-input 
              v-model="scope.row.targetName" 
              placeholder="目标字段"></el-input>
            </el-form-item>
          </template>
        </el-table-column>
      </el-table>
    </el-form>
  </div>
</template>
<script>
export default {
  name: '',
  components: {},
  props:{
    tableInfo:{
      type:Object,
      required:false,
      default:{}
    },
    isHasSnapshot:{
      type:Boolean,
      required:false,
      default:false
    }
  },
  data() {
    return {
      tableForm:{
        columnData:[{}],
      },
      oldColumnData:[],
      tableFormP:{
        partitionData:[],
      },
      oldPartitionData:[],
      tableFormRules:{
        columnName: [
          { required: true, message: '字段名称不允许为空', trigger: 'blur' },
        ],
        dataType: [
          { required: true, message: '字段类型不允许为空', trigger: 'change' },
        ],
        precision:[],
        scale:[],
        keyType:[],
        valueType:[]
      },
      tableFormRulesP:{
        sourceItem: [
          { required: true, message: '分区字段不允许为空', trigger: 'change' },
        ],
      },
      valueTypeList: [
        {value: 'STRING',label: 'STRING'},
        {value: 'FLOAT',label: 'FLOAT'},
        {value: 'DOUBLE',label: 'DOUBLE'},
        {value: 'LONG',label: 'LONG'},
        {value: 'INTEGER',label: 'INTEGER'},
        {value: 'BOOLEAN',label: 'BOOLEAN'},
        {value: 'Decimal',label: 'Decimal'},
        {value: 'Timestamp',label: 'Timestamp'},
        {value: 'List',label: 'List'},
        {value: 'Map',label: 'Map'},
      ],
      timeType: [
        {value: 'hour',label: 'hour'},
        {value: 'day',label: 'day'},
        {value: 'month',label: 'month'},
        {value: 'year',label: 'year'}
      ],
      selectListToDel:[],
      selectListToDelP:[],
    }
  },
  computed: {},
  watch: {},
  created() {
    if(this.tableInfo.tableName) {
      this.tableForm.columnData = JSON.parse(JSON.stringify(this.tableInfo.columnDtos))
      this.tableForm.columnData.map((item,index)=>{
        item.isNullable = item.isNullable == '1'
        item.isWithZone = item.isWithZone == '1'
        item.index = index
        return item
      })
      this.oldColumnData = JSON.parse(JSON.stringify(this.tableForm.columnData))

      this.tableFormP.partitionData = JSON.parse(JSON.stringify(this.tableInfo.partitionParams))
      this.tableFormP.partitionData.map((item,index)=>{
        item.sourceItem = {
          columnName:item.sourceName
        }
        item.index = index
        return item
      })
      this.oldPartitionData = JSON.parse(JSON.stringify(this.tableFormP.partitionData))
    }
  },
  methods: {
    handleAdd() {
      this.tableForm.columnData.push({
        'exectype':'1'
      })
    },
    handleAddP() {
      this.tableFormP.partitionData.push({
        'exectype':'1',
        'sourceItem':{columnName:''}
      })
    },
    // 多选框选中数据
    handleSelectionChange(selection) {
      this.selectListToDel = selection
    },
    handleSelectionChangeP(selection) {
      this.selectListToDelP = selection
    },
    selectable(row,rowIndex) {
      if(this.tableInfo.tableName && this.isHasSnapshot) {
        return false
      }else {
        return true
      }
    },
    handleChangeType(index) {
      if(this.tableForm.columnData[index].dataType != 'Decimal') {
        this.$set(this.tableForm.columnData[index],'precision',null)
        this.$set(this.tableForm.columnData[index],'scale',null)
      }else if(this.tableForm.columnData[index].dataType != 'Timestamp') {
        this.$set(this.tableForm.columnData[index],'isNullableSubType',null)
      }else if(this.tableForm.columnData[index].dataType != 'List') {
        this.$set(this.tableForm.columnData[index],'valueType',null)
      }else if(this.tableForm.columnData[index].dataType != 'Map') {
        this.$set(this.tableForm.columnData[index],'keyType',null)
        this.$set(this.tableForm.columnData[index],'valueType',null)
      }
    },
    handleChangeColumn(index) {
      if(this.tableFormP.partitionData[index].sourceItem.dataType != 'Timestamp') {
        this.$set(this.tableFormP.partitionData[index],'type','identity')
        // this.$set(this.tableFormP.partitionData[index],'targetName',this.tableFormP.partitionData[index].sourceItem.columnName)
      }else {
        this.$set(this.tableFormP.partitionData[index],'type',null)
        // this.$set(this.tableFormP.partitionData[index],'targetName','')
      }
    },
    validateField() {
      let result = true
      this.$refs.tableForm.validate((valid)=>{
        if(valid) {
          result = true
        }else {
          result = false
        }
      })
      return result
    },
    validateFieldP() {
      let result = true
      this.$refs.tableFormP.validate((valid)=>{
        if(valid) {
          result = true
        }else {
          result = false
        }
      })
      return result
    },
    handleDel() {
      this.selectListToDel.forEach(item=>{
        var index = this.tableForm.columnData.indexOf(item)
        if (index !== -1) {
          this.tableForm.columnData.splice(index, 1)
          if(this.tableInfo.tableName && item.exectype!='1') {
            this.oldColumnData[index] = {
              ...this.oldColumnData[index],
              'exectype':'3'
            }
          }
        }
      })
    },
    handleDelP() {
      this.selectListToDelP.forEach(item=>{
        var index = this.tableFormP.partitionData.indexOf(item)
        if (index !== -1) {
          this.tableFormP.partitionData.splice(index, 1)
          if(this.tableInfo.tableName && item.exectype!='1') {
            this.oldPartitionData[index] = {
              ...this.oldPartitionData[index],
              'exectype':'3'
            }
          }
        }
      })
    },
  }
}
</script>
<style lang="scss" scoped>
.columnTableTitle {
  display: flex;
  align-items: center;
  margin-bottom: 10px;
}
.el-form-item {
  margin: 16px 0;
}
</style>