package tech.mlsql.byzer_client_sdk.scala_lang.generator.node

import tech.mlsql.byzer_client_sdk.scala_lang.generator.hint.PythonHint
import tech.mlsql.byzer_client_sdk.scala_lang.generator._
import tech.mlsql.common.utils.serder.json.JSONTool

import java.util.UUID

case class PythonMeta(__meta: MetaMeta, _tag: Option[String], _isReady: Boolean, _autogenTableName: String,
                      _tableName: String,
                      _options: Map[String, OptionValue],
                      _input: String,
                      _cache: Boolean,
                      _confTable: Option[String],
                      _model: Option[String],
                      _schema: Option[String],
                      _env: Option[String],
                      _dataMode: String,
                      _runIn: String,
                      _code: Option[String],
                      _rawCode: Option[String]

                     )

class Python(parent: Byzer) extends BaseNode {

  private var _isReady = false
  private var _autogenTableName = UUID.randomUUID().toString.replaceAll("-", "")
  private var _tableName = _autogenTableName


  private var _options = new Options(this)

  private var _input = "command"
  private var _cache = true
  private var _confTable: Option[String] = None
  private var _model: Option[String] = None
  private var _schema: Option[String] = None
  private var _env: Option[String] = None
  private var _dataMode = "model"
  private var _runIn = "driver"
  private var _code: Option[String] = None
  private var _rawCode: Option[String] = None

  override def fromJson(json: String): BaseNode = {
    val v = JSONTool.parseJson[PythonMeta](json)
    _tag = v._tag
    _isReady = v._isReady
    _autogenTableName = v._autogenTableName
    _tableName = v._tableName
    _input = v._input
    _cache = v._cache
    _confTable = v._confTable
    _model = v._model
    _schema = v._schema
    _env = v._env
    _dataMode = v._dataMode
    _runIn = v._runIn
    _code = v._code
    _rawCode = v._rawCode
    _options = new Options(this)
    v._options.foreach { item =>
      _options.addWithQuotedStr(item._1, item._2)
    }
    this
  }

  override def toJson: String = {
    JSONTool.toJsonStr(PythonMeta(
      __meta = MetaMeta(getClass.getName),
      _tag = _tag,
      _isReady = _isReady,
      _autogenTableName = _autogenTableName,
      _tableName = _tableName,
      _input = _input,
      _cache = _cache,
      _confTable = _confTable,
      _model = _model,
      _schema = _schema,
      _env = _env,
      _dataMode = _dataMode,
      _runIn = _runIn,
      _code = _code,
      _rawCode = _rawCode,
      _options = _options.items
    ))
  }

  override def getTag: Option[String] = _tag

  def input(v: String) = {
    _input = v
    this
  }

  def output(v: String) = {
    _tableName = v
    this
  }

  def cache(v: Boolean) = {
    _cache = v
    this
  }

  def confTable(v: String) = {
    _confTable = Some(v)
    this
  }

  def model(v: String) = {
    _model = Some(v)
    this
  }

  def schema(v: String) = {
    _schema = Some(v)
    this
  }

  def env(v: String) = {
    _env = Some(v)
    this
  }

  def dataMode(v: String) = {
    _dataMode = v
    this
  }

  def runIn(v: String) = {
    _runIn = v
    this
  }

  def code(v: String) = {
    _code = Some(v)
    this
  }

  def codeWithHint(v: String) = {
    _rawCode = Some(v)
    this
  }

  override def tableName: String = {
    _tableName
  }

  override def namedTableName(tableName: String): BaseNode = {
    _tableName = tableName
    this
  }

  private var _tag: Option[String] = None

  override def tag(str: String): BaseNode = {
    _tag = Some(str)
    this
  }

  override def end: Byzer = {
    _isReady = true
    parent
  }

  override def options(): Options = {
    _options
  }

  override def toBlock: String = {
    require(_isReady, "end is not called")

    def confTableStr = {
      if (_confTable.isDefined) {
        s"""#%confTable=${_confTable.get}"""
      } else {
        ""
      }
    }

    def modelStr = {
      if (_model.isDefined) {
        s"""#%model=${_model.get}"""
      } else {
        ""
      }
    }

    def schemaStr = {
      if (_schema.isDefined) {
        s"""#%schema=${_schema.get}"""
      } else {
        ""
      }
    }

    def envStr = {
      if (_env.isDefined) {
        s"""#%env=${_env.get}"""
      } else {
        ""
      }
    }

    val finalCode = if (_rawCode.isDefined) {
      _rawCode.get
    } else {
      s"""#%python
         |#%input=${_input}
         |#%output=${_tableName}
         |#%cache=${_cache.toString}
         |${confTableStr}
         |${modelStr}
         |${schemaStr}
         |${envStr}
         |#%dataMode=${_dataMode}
         |#%runIn=${_runIn}
         |${_code.get}
         |""".stripMargin
    }
    val ph = new PythonHint
    ph.rewrite(finalCode, Map())
  }
}
