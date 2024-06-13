<template>
  <textarea ref="mycode" class="codesql" style="width:600px;" />
</template>

<script>
import sqlFormatter from 'sql-formatter'
import 'codemirror/theme/ambiance.css'
import 'codemirror/lib/codemirror.css'
import 'codemirror/addon/hint/show-hint.css'
const CodeMirror = require('codemirror/lib/codemirror')
require('codemirror/addon/edit/matchbrackets')
require('codemirror/addon/selection/active-line')
require('codemirror/mode/sql/sql')
require('codemirror/addon/hint/show-hint')
require('codemirror/addon/hint/sql-hint')
export default {
  name: 'SEditor',
  props: {
    sqlValue: {
      type: String
    }
  },
  data() {
    return {
      SEditor: false
    }
  },
  watch: {
    sqlValue(value) {
      const editorValue = this.SEditor.getValue()
      if (value !== editorValue) {
        this.SEditor.setValue(this.sqlValue)
      }
    }
  },
  mounted() {
    const mime = 'text/x-mysql'
    // let theme = 'ambiance'//设置主题，不设置的会使用默认主题
    this.SEditor = CodeMirror.fromTextArea(this.$refs.mycode, {
      mode: mime, // 选择对应代码编辑器的语言，我这边选的是数据库，根据个人情况自行设置即可
      indentWithTabs: true,
      smartIndent: true,
      lineNumbers: true,
      matchBrackets: true,
      // theme: theme,
      // autofocus: true,
      styleSelectedText: true,
      extraKeys: {
        'Ctrl': 'autocomplete',
        "'a'": completeAfter,
        "'b'": completeAfter,
        "'c'": completeAfter,
        "'d'": completeAfter,
        "'e'": completeAfter,
        "'f'": completeAfter,
        "'g'": completeAfter,
        "'h'": completeAfter,
        "'i'": completeAfter,
        "'j'": completeAfter,
        "'k'": completeAfter,
        "'l'": completeAfter,
        "'m'": completeAfter,
        "'n'": completeAfter,
        "'o'": completeAfter,
        "'p'": completeAfter,
        "'q'": completeAfter,
        "'r'": completeAfter,
        "'s'": completeAfter,
        "'t'": completeAfter,
        "'u'": completeAfter,
        "'v'": completeAfter,
        "'w'": completeAfter,
        "'x'": completeAfter,
        "'y'": completeAfter,
        "'z'": completeAfter,
        "'.'": completeAfter,
        "'='": completeflnTag,

        Tab: function(cm) {
          var spaces = Array(cm.getOption('indentUnit') + 1).join(' ')
          cm.replaceSelection(spaces)
        }
      } // 自定义快捷键
    })
    this.SEditor.setValue(this.sqlValue)
    this.SEditor.on('change', cm => {
      this.$emit('changed', cm.getValue())
      this.$emit('input', cm.getValue())
    })
    // 代码自动提示功能，记住使用cursorActivity事件不要使用change事件，这是一个坑，那样页面直接会卡死
    this.SEditor.on('cursorActivity', function() {
      this.SEditor.showHint()
      this.changeStatus()
    })
    function completeflnTag(cm, pred) {
      return completeAfter(cm, function() {
        const tok = cm.getTokenAt(cm.getCursor())
        if (tok.type === 'string' && (!/['"]/.test(tok.string.charAt(tok.string.length - 1)) || tok.string.length === 1)) return false
        const inner = CodeMirror.innerMode(cm.getMode(), tok.state).state
        return inner.tagName
      })
    }
    function completeAfter(cm, pred) {
      const cur = cm.getCursor()
      if (!pred || pred()) {
        setTimeout(function() {
          if (!cm.state.completionActive) {
            cm.showHint({
              completeSingle: false
            })
          }
        }, 100)
      }
      return CodeMirror.Pass
    }
  },
  methods: {
    getValue() {
      return this.SEditor.getValue()
    },
    setValue(val) {
      this.SEditor.setValue(val)
    },
    getSelectedValue() {
      return this.SEditor.getSelection()
    },
    formatSql() {
      let sqlContent = ''
      sqlContent = this.SEditor.getValue()
      return this.SEditor.setValue(sqlFormatter.format(sqlContent))
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
</style>
