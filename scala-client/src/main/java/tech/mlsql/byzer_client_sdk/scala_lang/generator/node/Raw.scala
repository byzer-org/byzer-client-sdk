package tech.mlsql.byzer_client_sdk.scala_lang.generator.node

import tech.mlsql.byzer_client_sdk.scala_lang.generator.{BaseNode, Byzer, MetaMeta, Options}
import tech.mlsql.common.utils.serder.json.JSONTool

import java.util.UUID

case class RawMeta(__meta: MetaMeta,
                   _tag: Option[String],
                   _isReady: Boolean,
                   _autogenTableName: String,
                   _code: String,
                   _tableName: String)

class Raw(parent: Byzer) extends BaseNode {
  private var _isReady = false
  private var _autogenTableName = UUID.randomUUID().toString.replaceAll("-", "")
  private var _tableName = _autogenTableName
  private var _code = ""

  def code(s: String) = {
    _code = s
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

  override def options(): Options = ???

  override def toBlock: String = {
    require(_isReady, "end is not called")
    _code
  }

  override def getTag: Option[String] = _tag

  override def fromJson(json: String): BaseNode = {
    val v = JSONTool.parseJson[RawMeta](json)
    _tag = v._tag
    _isReady = v._isReady
    _autogenTableName = v._autogenTableName
    _tableName = v._tableName
    _code ++= v._code
    this
  }

  override def toJson: String = {
    JSONTool.toJsonStr(RawMeta(
      __meta = MetaMeta(getClass.getName),
      _tag = _tag,
      _isReady = _isReady,
      _autogenTableName = _autogenTableName,
      _tableName = _tableName,
      _code = _code
    ))
  }
}