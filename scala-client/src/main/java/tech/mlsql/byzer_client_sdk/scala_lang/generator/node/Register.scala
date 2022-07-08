package tech.mlsql.byzer_client_sdk.scala_lang.generator.node

import tech.mlsql.byzer_client_sdk.scala_lang.generator._
import tech.mlsql.common.utils.serder.json.JSONTool

import java.util.UUID

class UDF(parent: Register) {
  def end = parent

  def name(s: String) = {
    parent._name = s
    this
  }

  def code(s: String) = {
    parent._code = Some(s)
    this
  }

  def tpe(s: UDFType) = {
    parent._tpe = s.sql
    this
  }

  def lang(s: LangType) = {
    parent._lang = s.sql
    this
  }

}

case class RegisterMeta(__meta: MetaMeta, _tag: Option[String], _isReady: Boolean, _autogenTableName: String,
                        _tableName: String,
                        _options: Map[String, OptionValue],
                        _name: String,
                        _tpe: String,
                        _lang: String,
                        _code: Option[String]
                       )

class Register(parent: Byzer) extends BaseNode {
  private var _isReady = false
  private var _autogenTableName = UUID.randomUUID().toString.replaceAll("-", "")
  private var _tableName = _autogenTableName

  private[generator] var _options = new Options(this)
  private[generator] var _name = "EmptyTable"
  private[generator] var _tpe = "udf"
  private[generator] var _lang = "scala"
  private[generator] var _code: Option[String] = None

  override def fromJson(json: String): BaseNode = {
    val v = JSONTool.parseJson[RegisterMeta](json)
    _tag = v._tag
    _isReady = v._isReady
    _autogenTableName = v._autogenTableName
    _tableName = v._tableName
    _name = v._name
    _tpe = v._tpe
    _code = v._code
    _lang = v._lang

    _options = new Options(this)
    v._options.foreach { item =>
      _options.addWithQuotedStr(item._1, item._2)
    }
    this
  }

  override def toJson: String = {
    JSONTool.toJsonStr(RegisterMeta(
      __meta = MetaMeta(getClass.getName),
      _tag = _tag,
      _isReady = _isReady,
      _autogenTableName = _autogenTableName,
      _tableName = _tableName,
      _name = _name,
      _tpe = _tpe,
      _code = _code,
      _lang = _lang,
      _options = _options.items
    ))
  }

  def udf() = {
    new UDF(this)
  }

  override def getTag: Option[String] = _tag

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
    s"""register ScriptUDF.`` as ${_name}
       |where lang="${_lang}"
       |and code='''${_code.get}'''
       |and udfType="${_tpe}";""".stripMargin
  }
}