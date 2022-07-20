package tech.mlsql.byzer_client_sdk.scala_lang.generator.node

import net.sf.json.{JSONArray, JSONObject}
import tech.mlsql.byzer_client_sdk.scala_lang.generator._
import tech.mlsql.common.utils.serder.json.JSONTool

import java.util.UUID
import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.mutable.ArrayBuffer

case class FilterMeta(__meta: MetaMeta, _tag: Option[String], _isReady: Boolean, _autogenTableName: String,
                      _tableName: String, _from: String,
                      _clauses: List[AndOrOptMeta])

class Filter(parent: Byzer) extends BaseNode {
  private var _isReady = false
  private var _autogenTableName = UUID.randomUUID().toString.replaceAll("-", "")
  private var _tableName = _autogenTableName
  private var _clauses = ArrayBuffer[BaseOpt]()
  private var _from = parent.lastTableName


  override def fromJson(json: String): BaseNode = {
    val obj = JSONObject.fromObject(json)
    val clauseTemp = obj.remove("_clauses")

    clauseTemp.asInstanceOf[JSONArray].asScala.foreach { item =>
      val t = item.asInstanceOf[JSONObject]
      val opt = Class.forName(t.getJSONObject("__meta").getString("name")).
        getConstructor(classOf[BaseNode]).
        newInstance(this).asInstanceOf[BaseOpt]
      opt match {
        case _ if opt.isInstanceOf[AndOpt[Filter]] =>
          val andOpt = opt.asInstanceOf[AndOpt[Filter]]
          t.getJSONArray("_clauses").asScala.map { t =>
            andOpt.add(AndOrTools.toFilterNode(t.asInstanceOf[JSONObject]))
          }
        case _ if opt.isInstanceOf[OrOpt[Filter]] =>
          val orOpt = opt.asInstanceOf[OrOpt[Filter]]
          t.getJSONArray("_clauses").asScala.map { t =>
            orOpt.add(AndOrTools.toFilterNode(t.asInstanceOf[JSONObject]))
          }
      }
      _clauses += opt
    }

    val v = JSONTool.parseJson[FilterMeta](obj.toString)
    _tag = v._tag
    _isReady = v._isReady
    _autogenTableName = v._autogenTableName
    _tableName = v._tableName
    _from = v._from
    this
  }

  override def toJson: String = {

    val clauses = _clauses.map { item =>
      item match {
        case _ if item.isInstanceOf[AndOpt[Filter]] =>
          AndOrOptMeta(MetaMeta(classOf[AndOpt[Filter]].getName), item.asInstanceOf[AndOpt[Filter]]._clauses.map { t =>
            AndOrTools.toFilterNodeMeta(t)
          }.toList)
        case _ if item.isInstanceOf[OrOpt[Filter]] =>
          AndOrOptMeta(MetaMeta(classOf[OrOpt[Filter]].getName), item.asInstanceOf[OrOpt[Filter]]._clauses.map { t =>
            AndOrTools.toFilterNodeMeta(t)
          }.toList)
      }
    }

    JSONTool.toJsonStr(FilterMeta(
      __meta = MetaMeta(getClass.getName),
      _tag = _tag,
      _isReady = _isReady,
      _autogenTableName = _autogenTableName,
      _tableName = _tableName,
      _from = _from,
      _clauses = clauses.toList
    ))
  }

  def or(): OrOpt[Filter] = {
    val n = new OrOpt(this)
    _clauses += n
    n
  }

  def and(): AndOpt[Filter] = {
    val n = new AndOpt(this)
    _clauses += n
    n
  }

  def from(tableName: String) = {
    _from = tableName
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

  override def options(): Options = throw new UnsupportedOperationException("options is not supported in filter")

  override def toBlock: String = {
    val cla = _clauses.map { cla => cla.toFilterNode.toFragment }.mkString(" and ")
    s"""select * from ${_from} where ${cla} as ${_tableName};"""
  }

  override def getTag: Option[String] = _tag
}