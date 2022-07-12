package tech.mlsql.byzer_client_sdk.scala_lang.generator.node

import tech.mlsql.byzer_client_sdk.scala_lang.generator._
import tech.mlsql.common.utils.serder.json.JSONTool

import java.util.UUID

case class SetMeta(__meta: MetaMeta, _tag: Option[String], _isReady: Boolean, _autogenTableName: String,
                   _tableName: String,
                   _options: Map[String, OptionValue],
                   _name: String,
                   _value: String,
                   _type: String,
                   _mode: String,
                   _lifeTime: String
                  )

/**
 * Byzer.variable.name(...).value(....).tpe(....).lifeTime(...).mode(...).options.add("...","....").end.end
 */
class Set(parent: Byzer) extends BaseNode {

  private var _isReady = false
  private var _autogenTableName = UUID.randomUUID().toString.replaceAll("-", "")
  private var _tableName = _autogenTableName

  private var _name = ""
  private var _value = ""
  private var _type = VariableTextType.sql
  private var _mode = VariableCompileMode.sql
  private var _lifeTime = VariableRequestLifeTime.sql

  private var _options = new Options(this)

  override def fromJson(json: String): BaseNode = {
    val v = JSONTool.parseJson[SetMeta](json)
    _tag = v._tag
    _isReady = v._isReady
    _autogenTableName = v._autogenTableName
    _tableName = v._tableName
    _name = v._name
    _value = v._value
    _type = v._type
    _mode = v._mode
    _lifeTime = v._lifeTime

    _options = new Options(this)
    v._options.foreach { item =>
      _options.addWithQuotedStr(item._1, item._2)
    }
    this
  }

  override def toJson: String = {
    JSONTool.toJsonStr(SetMeta(
      __meta = MetaMeta(getClass.getName),
      _tag = _tag,
      _isReady = _isReady,
      _autogenTableName = _autogenTableName,
      _tableName = _tableName,
      _name = _name,
      _value = _value,
      _type = _type,
      _mode = _mode,
      _lifeTime = _lifeTime,
      _options = _options.items
    ))
  }


  def name(expr: Expr) = {
    _name = expr.toFragment
    this
  }

  def value(expr: Expr) = {
    _value = expr.toFragment
    this
  }

  def tpe(v: VariableType) = {
    _type = v.sql
    this
  }

  def lifeTime(v: VariableLifeTime) = {
    _lifeTime = v.sql
    this
  }

  def mode(v: VariableMode) = {
    _mode = v.sql
    this
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
    _options.add("mode", _mode).add("scope", _lifeTime).add("type", _type)
    val opts = _options.toFragment
    val v = _type match {
      case "text" | "defaultParam" => if (_value.isEmpty) "\"\"" else s"'''${_value}'''"
      case "sql" | "shell" => s"`${_value}`"
      case "conf" => s"${_value}"
    }
    s"""set ${_name}=${v} ${opts};"""
  }
}