package tech.mlsql.byzer_client_sdk.scala_lang.generator.node

import tech.mlsql.byzer_client_sdk.scala_lang.generator._
import tech.mlsql.common.utils.serder.json.JSONTool

import java.util.UUID

sealed abstract class SaveMode {
  def sql: String
}

case object SaveOverwriteMode extends SaveMode {
  override def sql: String = "overwrite"
}

case object SaveAppendMode extends SaveMode {
  override def sql: String = "append"
}

case object SaveIgnoreMode extends SaveMode {
  override def sql: String = "ignore"
}

case object SaveErrorIfExistsMode extends SaveMode {
  override def sql: String = "errorIfExists"
}

case class SaveMeta(__meta: MetaMeta, _tag: Option[String], _isReady: Boolean, _autogenTableName: String,
                    _tableName: String,
                    _options: Map[String, OptionValue],
                    _format: Option[String],
                    _path: Option[String],
                    _mode: String,
                    _from: String
                   )

class Save(parent: Byzer) extends BaseNode {

  private var _isReady = false
  private var _autogenTableName = UUID.randomUUID().toString.replaceAll("-", "")
  private var _tableName = _autogenTableName

  private var _format: Option[String] = None
  private var _path: Option[String] = None

  private var _options = new Options(this)

  private var _mode = SaveAppendMode.sql
  private var _from = "command"

  override def getTag: Option[String] = _tag

  override def fromJson(json: String): BaseNode = {
    val v = JSONTool.parseJson[SaveMeta](json)
    _tag = v._tag
    _isReady = v._isReady
    _autogenTableName = v._autogenTableName
    _tableName = v._tableName
    _format = v._format
    _path = v._path
    _mode = v._mode
    _from = v._from
    _options = new Options(this)
    v._options.foreach { item =>
      _options.addWithQuotedStr(item._1, item._2)
    }
    this
  }

  override def toJson: String = {
    JSONTool.toJsonStr(SaveMeta(
      __meta = MetaMeta(getClass.getName),
      _tag = _tag,
      _isReady = _isReady,
      _autogenTableName = _autogenTableName,
      _tableName = _tableName,
      _format = _format,
      _path = _path,
      _mode = _mode,
      _from = _from,
      _options = _options.items
    ))
  }


  def from(v: String) = {
    _from = v
    this
  }

  def mode(v: SaveMode) = {
    _mode = v.sql
    this
  }

  def format(s: String) = {
    _format = Some(s)
    this
  }

  def path(s: String) = {
    _path = Some(s)
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
    s"""save ${_mode} ${_from} as ${_format.get}.`${_path.getOrElse("")}` ${_options.toFragment};"""
  }
}

