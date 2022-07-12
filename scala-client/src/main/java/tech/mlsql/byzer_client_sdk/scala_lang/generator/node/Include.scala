package tech.mlsql.byzer_client_sdk.scala_lang.generator.node

import tech.mlsql.byzer_client_sdk.scala_lang.generator.{BaseNode, Byzer, MetaMeta, Options}
import tech.mlsql.common.utils.serder.json.JSONTool

import java.util.UUID

/**
 *
 * // include Byzer lib
 * Byzer().include.lib(...).commit(...).alias(...).forceUpdate(....).end
 * // include Byzer package
 * Byzer().include.package(...).end
 */
case class IncludeMeta(__meta: MetaMeta, _tag: Option[String], _isReady: Boolean, _autogenTableName: String,
                       _tableName: String,
                       _lib: String,
                       _commit: Option[String],
                       _alias: String,
                       _forceUpdate: Boolean,
                       _package: String,
                       _path: String,
                       _mode: String,
                       _libMirror: Option[String]
                      )

class Include(parent: Byzer) extends BaseNode {

  private var _isReady = false
  private var _autogenTableName = UUID.randomUUID().toString.replaceAll("-", "")
  private var _tableName = _autogenTableName

  private var _lib = ""
  private[generator] var _commit: Option[String] = None
  private[generator] var _alias = ""
  private[generator] var _forceUpdate = false

  private[generator] var _package = ""
  private[generator] var _path = ""

  private[generator] var _mode = "lib"

  private[generator] var _libMirror: Option[String] = None

  override def fromJson(json: String): BaseNode = {
    val v = JSONTool.parseJson[IncludeMeta](json)
    _tag = v._tag
    _isReady = v._isReady
    _autogenTableName = v._autogenTableName
    _tableName = v._tableName
    _lib = v._lib
    _commit = v._commit
    _forceUpdate = v._forceUpdate
    _package = v._package
    _path = v._path
    _mode = v._mode
    _libMirror = v._libMirror
    _alias = v._alias
    this
  }

  override def toJson: String = {
    JSONTool.toJsonStr(IncludeMeta(
      __meta = MetaMeta(getClass.getName),
      _tag = _tag,
      _isReady = _isReady,
      _autogenTableName = _autogenTableName,
      _tableName = _tableName,
      _lib = _lib,
      _commit = _commit,
      _forceUpdate = _forceUpdate,
      _package = _package,
      _path = _path,
      _mode = _mode,
      _libMirror = _libMirror,
      _alias = _alias
    ))
  }

  def lib(s: String) = {
    _lib = s
    _mode = "lib"
    new LibInclude(this)
  }

  override def getTag: Option[String] = _tag

  def `package`(s: String) = {
    _package = s
    _mode = "local"
    new PackageInclude(this)
  }

  def project(s: String) = {
    _path = s
    _mode = "project"
    new ProjectInclude(this)
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

  private val _option = new Options(this)

  override def options(): Options = {
    _option
  }

  override def toBlock: String = {
    var path = _path
    _mode match {
      case "lib" =>
        require(!_alias.isEmpty, "alias is required in include statement")
        if (_commit.isDefined) {
          _option.add("commit", _commit.get)
        }
        _option.add("forceUpdate", _forceUpdate.toString)
        _option.add("alias", _alias)
        path = _lib
      case "project" =>
        path = _path

      case "local" =>
        path = _package
    }
    s"""include ${_mode}.`${path}` ${_option.toFragment};"""
  }
}

class LibInclude(parent: Include) {
  def commit(s: String) = {
    parent._commit = Some(s)
    this
  }

  def alias(s: String) = {
    parent._alias = s
    this
  }

  def forceUpdate(s: Boolean) = {
    parent._forceUpdate = s
    this
  }

  def end = {
    parent
  }

  def libMirror(s: String) = {
    parent._libMirror = Some(s)
    this
  }
}

class PackageInclude(parent: Include) {
  def end = {
    parent
  }
}

class ProjectInclude(parent: Include) {

  def path(s: String) = {
    parent._path = s
    this
  }

  def end = {
    parent
  }
}
