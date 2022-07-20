package tech.mlsql.byzer_client_sdk.scala_lang.generator.node

import tech.mlsql.byzer_client_sdk.scala_lang.generator.{BaseNode, Byzer, MetaMeta, Options}
import tech.mlsql.common.utils.serder.json.JSONTool

import java.util.UUID
import scala.collection.mutable.ArrayBuffer

sealed abstract class OrderType {
  def sql: String
}

case object OrderDescType extends OrderType {
  override def sql: String = "desc"
}

case object OrderAscType extends OrderType {
  override def sql: String = "asc"
}

case class OrderTuple(col: String, tpe: String)

case class OrderByMeta(__meta: MetaMeta,
                       _tag: Option[String],
                       _isReady: Boolean,
                       _autogenTableName: String,
                       _from: String,
                       _tableName: String, _orderTuples: List[OrderTuple])

class OrderBy(parent: Byzer) extends BaseNode {
  private var _isReady = false
  private var _autogenTableName = UUID.randomUUID().toString.replaceAll("-", "")
  private var _tableName = _autogenTableName
  private var _orderTuples = ArrayBuffer[OrderTuple]()
  private var _from = parent.lastTableName

  def from(s: String) = {
    _from = s
    this
  }

  def add(col: String, tpe: OrderType) = {
    _orderTuples += OrderTuple(col, tpe.sql)
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
    if (parent.blocks.size == 1 && _from.isEmpty)
      throw new RuntimeException("First Node must call def from()!")
    _isReady = true
    parent
  }

  override def options(): Options = ???

  override def toBlock: String = {
    require(_isReady, "end is not called")
    val v = _orderTuples.map { item =>
      s"${item.col} ${item.tpe}"
    }.mkString(",")
    s"""select * from ${_from} order by ${v};"""
  }

  override def getTag: Option[String] = _tag

  override def fromJson(json: String): BaseNode = {
    val v = JSONTool.parseJson[OrderByMeta](json)
    _tag = v._tag
    _isReady = v._isReady
    _autogenTableName = v._autogenTableName
    _tableName = v._tableName
    _orderTuples ++= v._orderTuples
    this
  }

  override def toJson: String = {
    JSONTool.toJsonStr(OrderByMeta(
      __meta = MetaMeta(getClass.getName),
      _tag = _tag,
      _isReady = _isReady,
      _autogenTableName = _autogenTableName,
      _tableName = _tableName,
      _orderTuples = _orderTuples.toList,
      _from = _from
    ))
  }
}
