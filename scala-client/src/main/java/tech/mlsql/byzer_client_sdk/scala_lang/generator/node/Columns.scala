package tech.mlsql.byzer_client_sdk.scala_lang.generator.node

import tech.mlsql.byzer_client_sdk.scala_lang.generator._
import tech.mlsql.common.utils.serder.json.JSONTool

import java.util.UUID
import scala.collection.mutable.ArrayBuffer

case class ColumnsMeta(__meta: MetaMeta, _tag: Option[String], _isReady: Boolean, _autogenTableName: String,
                       _tableName: String, _from: String,
                       _columns: List[Expr])

class Columns(parent: Byzer) extends BaseNode {
  private var _isReady = false
  private var _autogenTableName = UUID.randomUUID().toString.replaceAll("-", "")
  private var _tableName = _autogenTableName

  private var _from = parent.lastTableName

  private var _columns = ArrayBuffer[Expr]()

  override def fromJson(json: String): BaseNode = {
    val v = JSONTool.parseJson[ColumnsMeta](json)
    _tag = v._tag
    _isReady = v._isReady
    _autogenTableName = v._autogenTableName
    _tableName = v._tableName
    _from = v._from
    _columns ++= v._columns
    this
  }

  override def toJson: String = {
    JSONTool.toJsonStr(ColumnsMeta(
      __meta = MetaMeta(getClass.getName),
      _tag = _tag,
      _isReady = _isReady,
      _autogenTableName = _autogenTableName,
      _tableName = _tableName,
      _from = _from,
      _columns = _columns.toList
    ))
  }


  def addColumn(expr: Expr): Columns = {
    _columns += expr
    this
  }

  override def getTag: Option[String] = _tag

  def from(tableName: String) = {
    _from = tableName
    this
  }

  private var _tag: Option[String] = None

  override def tag(str: String): BaseNode = {
    _tag = Some(str)
    this
  }

  override def tableName: String = {
    _tableName
  }

  override def namedTableName(tableName: String): BaseNode = {
    _tableName = tableName
    this
  }

  override def end: Byzer = {
    if (parent.blocks.size == 1 && _from.isEmpty)
      throw new RuntimeException("First Node must call def from()!")
    _isReady = true
    parent
  }


  override def options(): Options = ???

  override def toBlock: String = {
    val project = _columns.map(_.toFragment).mkString(",")
    s"""select ${project} from ${_from} as ${_tableName};"""
  }
}
