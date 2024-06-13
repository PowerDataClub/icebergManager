<template>
  <textarea ref="mycode" class="codesql" style="width:100%;height:500px"/>
</template>

<script>
import 'codemirror/theme/ambiance.css'
import 'codemirror/lib/codemirror.css'
import 'codemirror/addon/hint/show-hint.css'

// language
import 'codemirror/mode/javascript/javascript'
import 'codemirror/addon/lint/lint.css'
import 'codemirror/addon/lint/json-lint'

// 折叠

import 'codemirror/addon/fold/xml-fold.js'
import 'codemirror/addon/fold/indent-fold.js'
import 'codemirror/addon/fold/markdown-fold.js'
import 'codemirror/addon/fold/foldgutter.css'
import 'codemirror/addon/fold/foldcode.js'
import 'codemirror/addon/fold/foldgutter.js'
import 'codemirror/addon/fold/brace-fold.js'
import 'codemirror/addon/fold/comment-fold.js'


const CodeMirror = require('codemirror/lib/codemirror')
export default {
  name: 'JsonEditor',
  props: {
    jsonValue: {
      type: String
    },
    readOnly:{
      type: Boolean,
      default:false
    }
  },
  data() {
    return {
      JsonEditor: false
    }
  },
  watch: {
    jsonValue(value) {
      const editorValue = this.JsonEditor.getValue()
      if (value !== editorValue) {
        this.JsonEditor.setValue(this.jsonValue)
      }
    }
  },
  mounted() {
    const mime = 'application/json'
    // let theme = 'ambiance'//设置主题，不设置的会使用默认主题
    this.JsonEditor = CodeMirror.fromTextArea(this.$refs.mycode, {
      mode: mime, // 选择对应代码编辑器的语言，我这边选的是数据库，根据个人情况自行设置即可
      readOnly:this.readOnly || false,
      indentWithTabs: true,
      smartIndent: true,
      lineNumbers: true,
      // theme: theme,
      // autofocus: true,
      styleSelectedText: true,
      extraKeys: {'Ctrl': 'autocomplete',}, // 自定义快捷键
      matchBrackets:true,//光标匹配库括号
      foldGutter:true,
      autoCloseTags:true,
      matchTags:{bothTags:true},
      lineWrapping:true,
      gutters:['CodeMirror-lint-markers','CodeMirror-linenumbers','CodeMirror-foldgutter'],
      // foldGutter:{
      //   rangeFinder: new CodeMirror.fold.combine(CodeMirror.fold.indent,CodeMirror.fold.comment)
      // }
    })
    this.JsonEditor.setValue(this.jsonValue)
    this.JsonEditor.on('change', cm => {
      this.$emit('changed', cm.getValue())
      this.$emit('input', cm.getValue())
    })
  },
  methods: {
    getValue() {
      return this.JsonEditor.getValue()
    },
    setValue(val) {
      this.JsonEditor.setValue(val)
    },
    getSelectedValue() {
      return this.JsonEditor.getSelection()
    },
    changeStatus() {
      this.$parent.changeSaveStatus()
    }
  }
}
</script>

<style>
.codesql {
  font-size: 11pt;
  font-family: Consolas, Menlo, Monaco, Lucida Console, Liberation Mono, DejaVu Sans Mono, Bitstream Vera Sans Mono, Courier New, monospace, serif;
}
.CodeMirror {
  height: 100%!important;
}
</style>
