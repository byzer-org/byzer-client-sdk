package tech.mlsql.byzer_client_sdk.scala_lang.generator.node

import tech.mlsql.byzer_client_sdk.scala_lang.generator.{BaseNode, Byzer, Expr, MetaMeta, Options}
import tech.mlsql.common.utils.serder.json.JSONTool

import java.util.UUID
import scala.collection.mutable.ArrayBuffer

class Agg(parent: GroupBy) {
  def end = parent

  def addColumn(expr: Expr) = {
    parent._aggs += expr
    this
  }
}

case class GroupByMeta(__meta: MetaMeta, _tag: Option[String], _isReady: Boolean, _autogenTableName: String,
                       _tableName: String, _from: String,
                       _groups: List[Expr],
                       _aggs: List[Expr]
                      )

class GroupBy(parent: Byzer) extends BaseNode {

  private var _isReady = false
  private var _autogenTableName = UUID.randomUUID().toString.replaceAll("-", "")
  private var _tableName = _autogenTableName
  private var _groups = ArrayBuffer[Expr]()
  private[generator] var _aggs = ArrayBuffer[Expr]()

  private var _from = parent.lastTableName

  override def fromJson(json: String): BaseNode = {
    val v = JSONTool.parseJson[GroupByMeta](json)
    _tag = v._tag
    _isReady = v._isReady
    _autogenTableName = v._autogenTableName
    _tableName = v._tableName
    _from = v._from
    _groups ++= v._groups
    _aggs ++= v._aggs
    this
  }

  override def toJson: String = {
    JSONTool.toJsonStr(GroupByMeta(
      __meta = MetaMeta(getClass.getName),
      _tag = _tag,
      _isReady = _isReady,
      _autogenTableName = _autogenTableName,
      _tableName = _tableName,
      _from = _from,
      _groups = _groups.toList,
      _aggs = _aggs.toList
    ))
  }


  def from(expr: Expr) = {
    _from = expr.toFragment
    this
  }

  def addColumn(expr: Expr) = {
    _groups += expr
    this
  }

  def agg() = {
    new Agg(this)
  }

  override def tableName: String = {
    _tableName
  }

  override def getTag: Option[String] = _tag

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

  override def options(): Options = ???

  override def toBlock: String = {
    val groups = _groups.map(_.expr.get).mkString(",")
    val aggs = _aggs.map(_.expr.get).mkString(",")
    s"""select ${aggs} from ${_from} group by ${groups};"""
  }
}
