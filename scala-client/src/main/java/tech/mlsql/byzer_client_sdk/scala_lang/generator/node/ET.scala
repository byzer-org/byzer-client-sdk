package tech.mlsql.byzer_client_sdk.scala_lang.generator.node

import tech.mlsql.byzer_client_sdk.scala_lang.generator._
import tech.mlsql.common.utils.serder.json.JSONTool

import java.util.UUID

case class ETMeta(__meta: MetaMeta, _tag: Option[String], _isReady: Boolean, _autogenTableName: String,
                  _tableName: String,
                  _options: Map[String, OptionValue],
                  _name: String,
                  _from: String,
                  _path: String,
                  _etType: String
                 )
/**
 *
 * Byzer().mod.run.from(...).name(...).end
 */
class ET(parent: Byzer) extends BaseNode {
  private var _isReady = false
  private var _autogenTableName = UUID.randomUUID().toString.replaceAll("-", "")
  private var _tableName = _autogenTableName

  private var _from = parent.lastTableName
  private var _options = new Options(this)
  private var _name = "EmptyTable"
  private var _path = ""
  private var _etType = "run"

  override def fromJson(json: String): BaseNode = {
    val v = JSONTool.parseJson[ETMeta](json)
    _tag = v._tag
    _isReady = v._isReady
    _autogenTableName = v._autogenTableName
    _tableName = v._tableName
    _name = v._name
    _from = v._from
    _path = v._path
    _etType = v._etType

    _options = new Options(this)
    v._options.foreach { item =>
      _options.addWithQuotedStr(item._1, item._2)
    }
    this
  }

  override def toJson: String = {
    JSONTool.toJsonStr(ETMeta(
      __meta = MetaMeta(getClass.getName),
      _tag = _tag,
      _isReady = _isReady,
      _autogenTableName = _autogenTableName,
      _tableName = _tableName,
      _name = _name,
      _from = _from,
      _path = _path,
      _etType = _etType,
      _options = _options.items
    ))
  }


  def from(expr: Expr) = {
    _from = expr.toFragment
    this
  }

  def train = {
    _etType = "train"
    this
  }

  def run = {
    _etType = "run"
    this
  }

  def predict = {
    _etType = "predict"
    this
  }

  def name(expr: Expr): ET = {
    _name = expr.toFragment
    this
  }

  def path(expr: Expr): ET = {
    _path = expr.toFragment
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
    if (parent.blocks.size == 1 && _from.isEmpty)
      throw new RuntimeException("First Node must call def from()!")
    _isReady = true
    parent
  }

  override def options(): Options = {
    _options
  }

  override def toBlock: String = {
    s"""${_etType} ${_from} as ${_name}.`${_path}` ${_options.toFragment} as ${_tableName};"""
  }
}
