package tech.mlsql.byzer_client_sdk.scala_lang.generator

import tech.mlsql.byzer_client_sdk.scala_lang.generator.hint.PythonHint

import java.util.UUID
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

// Byzer()
// .load.format("csv").path("/tmp/jack").end
// .select.project("").from("").where("").groupBy("").end
// .save.format("").path("").end.
// toScript

// val load = Byzer().load.format("csv").path("/tmp/jack")
// load.tableName
object Byzer {
  def apply() = {
    new Byzer()
  }
}

class Byzer {
  private val blocks = new ArrayBuffer[BaseNode]()
  private var _cluster = new Cluster(this)

  def getByTag(name: String): List[BaseNode] = {
    blocks.filter(_.getTag.isDefined).filter(_.getTag.get == name).toList
  }

  def getUntilTag(name: String): List[BaseNode] = {
    blocks.zipWithIndex.filter(_._1.getTag.isDefined).filter(_._1.getTag.get == name).headOption match {
      case Some(item) => blocks.slice(item._2, blocks.size).toList
      case None => List()
    }
  }

  def run() = {
    _cluster.getMatchedEngines.map { engine =>
      engine.run()
    }
  }

  def cluster() = {
    _cluster
  }

  def load = {
    val block = new Load(this)
    blocks += block
    block
  }

  def python = {
    val block = new Python(this)
    blocks += block
    block
  }

  def register = {
    val block = new Register(this)
    blocks += block
    block
  }

  def save = {
    val block = new Save(this)
    blocks += block
    block
  }

  def include = {
    val block = new Include(this)
    blocks += block
    block
  }

  def filter = {
    val block = new Filter(this)
    blocks += block
    block
  }

  def columns = {
    val block = new Columns(this)
    blocks += block
    block
  }

  def join = {
    val block = new Join(this)
    blocks += block
    block
  }

  def mod = {
    val block = new ET(this)
    blocks += block
    block
  }

  def variable = {
    val block = new Set(this)
    blocks += block
    block
  }

  def toScript: String = {
    blocks.map(item => item.toBlock).mkString("\n")
  }

  def lastTableName = {
    blocks.last.tableName
  }

}

trait BaseNode {
  def tableName: String

  def namedTableName(tableName: String): BaseNode

  def tag(str: String): BaseNode

  def end: Byzer

  def options(): Options

  def toBlock: String

  def getTag: Option[String]
}

case class OptionValue(value: String, quoteStr: Option[String])

class Options(parent: BaseNode) {
  private val o = new mutable.HashMap[String, OptionValue]()

  def add(name: String, value: String): Options = {
    o.put(name, OptionValue(value, None))
    this
  }

  def addWithQuotedStr(name: String, value: OptionValue) = {
    o.put(name, value)
    this
  }

  def end = {
    parent
  }

  def items = o.toMap

  def toFragment = {
    val opts = o.map { case (k, v) =>
      if (v.quoteStr.isEmpty) {
        if (v.value.isEmpty) {
          s"""`${k}`="""""
        } else {
          s"""`${k}`='''${v.value}'''"""
        }
      } else {
        s"""`${k}`=${v.quoteStr}${v}${v.quoteStr}"""
      }

    }.mkString(" and ")

    val optStr = if (!o.isEmpty) {
      s"""where ${opts}"""
    } else {
      ""
    }
    optStr
  }
}


class Load(parent: Byzer) extends BaseNode {

  private var _isReady = false
  private val _autogenTableName = UUID.randomUUID().toString.replaceAll("-", "")
  private var _tableName = _autogenTableName

  private var _format: Option[String] = None
  private var _path: Option[String] = None

  private var _options: Options = new Options(this)

  def format(s: String) = {
    _format = Some(s)
    this
  }

  def path(s: String) = {
    _path = Some(s)
    this
  }

  override def tableName: String = {
    require(_isReady, "end is not called")
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
    require(_isReady, "end is not called")
    s"""load ${_format.get}.`${_path.getOrElse("")}` ${_options.toFragment} as ${_tableName};"""
  }

  override def getTag: Option[String] = _tag
}

trait FilterNode {

  def toFragment = {
    visit(this)
  }

  def visit(node: FilterNode): String = {
    if (node.isInstanceOf[And]) {
      val n = node.asInstanceOf[And]
      s"""(${visit(n.left)} and ${visit(n.right)})"""
    } else if (node.isInstanceOf[Or]) {
      val n = node.asInstanceOf[Or]
      s"""(${visit(n.left)} or ${visit(n.right)})"""
    } else {
      node.asInstanceOf[Expr].expr.get
    }
  }
}

case class And(left: FilterNode, right: FilterNode) extends FilterNode

case class Or(left: FilterNode, right: FilterNode) extends FilterNode

case class Expr(expr: Option[String]) extends FilterNode

trait BaseOpt {
  def toFilterNode: FilterNode
}

class AndOpt(parent: Filter) extends BaseOpt {

  private var _isReady = false
  private val _clauses = ArrayBuffer[FilterNode]()


  def add(filterNode: FilterNode) = {
    _clauses += filterNode
    this
  }

  def end: Filter = {
    _isReady = true
    parent
  }

  override def toFilterNode: FilterNode = {
    val n = _clauses.reduce((a, b) => And(a, b))
    n
  }
}

class OrOpt(parent: Filter) extends BaseOpt {

  private var _isReady = false
  private val _autogenTableName = UUID.randomUUID().toString.replaceAll("-", "")
  private var _tableName = _autogenTableName
  private var _clauses = ArrayBuffer[FilterNode]()


  def add(filterNode: FilterNode) = {
    _clauses += filterNode
    this
  }

  def tableName: String = {
    require(_isReady, "end is not called")
    _tableName
  }

  def namedTableName(tableName: String): OrOpt = {
    _tableName = tableName
    this
  }

  def desc(str: String): BaseNode = ???

  def end: Filter = {
    _isReady = true
    parent
  }

  override def toFilterNode: FilterNode = {
    val n = _clauses.reduce((a, b) => Or(a, b))
    n
  }
}

class Filter(parent: Byzer) extends BaseNode {
  private var _isReady = false
  private val _autogenTableName = UUID.randomUUID().toString.replaceAll("-", "")
  private var _tableName = _autogenTableName
  private var _clauses = ArrayBuffer[BaseOpt]()
  private var _from = parent.lastTableName

  def or(): OrOpt = {
    val n = new OrOpt(this)
    _clauses += n
    n
  }

  def and(): AndOpt = {
    val n = new AndOpt(this)
    _clauses += n
    n
  }

  def from(tableName: String) = {
    _from = tableName
    this
  }

  override def tableName: String = {
    require(_isReady, "end is not called")
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

  override def options(): Options = throw new UnsupportedOperationException("options is not supported in filter")

  override def toBlock: String = {
    val cla = _clauses.map { cla => cla.toFilterNode.toFragment }.mkString(" and ")
    s"""select * from ${_from} where ${cla} as ${_tableName};"""
  }

  override def getTag: Option[String] = _tag
}


class Columns(parent: Byzer) extends BaseNode {
  private var _isReady = false
  private val _autogenTableName = UUID.randomUUID().toString.replaceAll("-", "")
  private var _tableName = _autogenTableName

  private var _from = parent.lastTableName

  private var _columns = ArrayBuffer[Expr]()

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
    require(_isReady, "end is not called")
    _tableName
  }

  override def namedTableName(tableName: String): BaseNode = {
    _tableName = tableName
    this
  }

  override def end: Byzer = {
    _isReady = true
    parent
  }


  override def options(): Options = ???

  override def toBlock: String = {
    val project = _columns.map(_.toFragment).mkString(",")
    s"""select ${project} from ${_from} as ${_tableName};"""
  }
}

/**
 * Byzer().join.from(...).left(...).on(...).leftColumns(....).rightColumns(.....)
 */
class Join(parent: Byzer) extends BaseNode {

  private var _isReady = false
  private val _autogenTableName = UUID.randomUUID().toString.replaceAll("-", "")
  private var _tableName = _autogenTableName

  private var _from = parent.lastTableName

  private var _joinTable: Option[String] = None
  private var _on: Option[String] = None
  private var _leftColumns: Option[String] = None
  private var _rightColumns: Option[String] = None

  def from(expr: Expr) = {
    _from = expr.toFragment
    this
  }

  override def tableName: String = {
    require(_isReady, "end is not called")
    _tableName
  }

  override def namedTableName(tableName: String): BaseNode = {
    _tableName = tableName
    this
  }

  override def getTag: Option[String] = _tag

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
    s"""select ${_leftColumns.get},${_rightColumns.get} from ${_from} ${_joinTable.get} on ${_on.get};"""
  }


  def leftColumns(expr: Expr) = {
    _leftColumns = Some(expr.toFragment)
    this
  }

  def rightColumns(expr: Expr) = {
    _rightColumns = Some(expr.toFragment)
    this
  }

  def on(expr: Expr) = {
    _on = Some(expr.toFragment)
    this
  }

  def left(expr: Expr) = {
    _joinTable = Some(s"""${LeftOuter.sql} ${expr.toFragment}""")
    this
  }

  def right(expr: Expr) = {
    _joinTable = Some(s"""${RightOuter.sql} ${expr.toFragment}""")
    this
  }

  def inner(expr: Expr) = {
    _joinTable = Some(s"""${Inner.sql} ${expr.toFragment}""")
    this
  }

  def fullouter(expr: Expr) = {
    _joinTable = Some(s"""${FullOuter.sql} ${expr.toFragment}""")
    this
  }

  def leftsemi(expr: Expr) = {
    _joinTable = Some(s"""${LeftSemi.sql} ${expr.toFragment}""")
    this
  }

  def leftanti(expr: Expr) = {
    _joinTable = Some(s"""${LeftAnti.sql} ${expr.toFragment}""")
    this
  }

  def cross(expr: Expr) = {
    _joinTable = Some(s"""${Cross} ${expr.toFragment}""")
    this
  }


}

class Agg(parent: GroupBy) {
  def end = parent

  def addColumn(expr: Expr) = {
    parent._aggs += expr
    this
  }
}

class GroupBy(parent: Byzer) extends BaseNode {

  private var _isReady = false
  private val _autogenTableName = UUID.randomUUID().toString.replaceAll("-", "")
  private var _tableName = _autogenTableName
  private var _groups = ArrayBuffer[Expr]()
  private[generator] var _aggs = ArrayBuffer[Expr]()

  private var _from = parent.lastTableName

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
    require(_isReady, "end is not called")
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

sealed abstract class UDFType {
  def sql: String
}

case object UDFUDFType extends UDFType {
  override def sql: String = "udf"
}

case object UDFUDAFType extends UDFType {
  override def sql: String = "udaf"
}

sealed abstract class LangType {
  def sql: String
}

case object LangScalaType extends LangType {
  override def sql: String = "scala"
}

case object LangJavaType extends LangType {
  override def sql: String = "java"
}

class Register(parent: Byzer) extends BaseNode {
  private var _isReady = false
  private val _autogenTableName = UUID.randomUUID().toString.replaceAll("-", "")
  private var _tableName = _autogenTableName

  private[generator] var _options = new Options(this)
  private[generator] var _name = "EmptyTable"
  private[generator] var _tpe = "udf"
  private[generator] var _lang = "scala"
  private[generator] var _code: Option[String] = None

  def udf() = {
    new UDF(this)
  }

  override def getTag: Option[String] = _tag

  override def tableName: String = {
    require(_isReady, "end is not called")
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

/**
 *
 * Byzer().mod.run.from(...).name(...).end
 */
class ET(parent: Byzer) extends BaseNode {
  private var _isReady = false
  private val _autogenTableName = UUID.randomUUID().toString.replaceAll("-", "")
  private var _tableName = _autogenTableName

  private var _from = parent.lastTableName
  private var _options = new Options(this)
  private var _name = "EmptyTable"
  private var _path = ""
  private var _etType = "run"

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
    require(_isReady, "end is not called")
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
    s"""${_etType} ${_from} as ${_name}.`${_path}` ${_options.toFragment} as ${_tableName};"""
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

/**
 *
 * // include Byzer lib
 * Byzer().include.lib(...).commit(...).alias(...).forceUpdate(....).end
 * // include Byzer package
 * Byzer().include.package(...).end
 */
class Include(parent: Byzer) extends BaseNode {

  private var _isReady = false
  private val _autogenTableName = UUID.randomUUID().toString.replaceAll("-", "")
  private var _tableName = _autogenTableName

  private var _lib = ""
  private[generator] var _commit: Option[String] = None
  private[generator] var _alias = ""
  private[generator] var _forceUpdate = false

  private[generator] var _package = ""
  private[generator] var _path = ""

  private[generator] var _mode = "lib"

  private[generator] var _libMirror: Option[String] = None

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
    require(_isReady, "end is not called")
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

sealed abstract class VariableType {
  def sql: String
}

case object VariableSQLType extends VariableType {
  override def sql: String = "sql"
}

case object VariableShellType extends VariableType {
  override def sql: String = "shell"
}

case object VariableTextType extends VariableType {
  override def sql: String = "text"
}

case object VariableConfType extends VariableType {
  override def sql: String = "conf"
}

case object VariableDefaultParamType extends VariableType {
  override def sql: String = "defaultParam"
}

sealed abstract class VariableLifeTime {
  def sql: String
}

case object VariableSessionLifeTime extends VariableLifeTime {
  override def sql: String = "session"
}

case object VariableRequestLifeTime extends VariableLifeTime {
  override def sql: String = "request"
}

sealed abstract class VariableMode {
  def sql: String
}

case object VariableCompileMode extends VariableMode {
  override def sql: String = "compile"
}

case object VariableRuntimeMode extends VariableMode {
  override def sql: String = "Runtime"
}


/**
 * Byzer.variable.name(...).value(....).tpe(....).lifeTime(...).mode(...).options.add("...","....").end.end
 */
class Set(parent: Byzer) extends BaseNode {

  private var _isReady = false
  private val _autogenTableName = UUID.randomUUID().toString.replaceAll("-", "")
  private var _tableName = _autogenTableName

  private var _name = ""
  private var _value = ""
  private var _type = VariableTextType.sql
  private var _mode = VariableCompileMode.sql
  private var _lifeTime = VariableRequestLifeTime.sql

  private var _options = new Options(this)

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
    require(_isReady, "end is not called")
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

class Save(parent: Byzer) extends BaseNode {

  private var _isReady = false
  private val _autogenTableName = UUID.randomUUID().toString.replaceAll("-", "")
  private var _tableName = _autogenTableName

  private var _format: Option[String] = None
  private var _path: Option[String] = None

  private var _options = new Options(this)

  private var _mode = SaveAppendMode.sql
  private var _from = "command"

  override def getTag: Option[String] = _tag

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
    require(_isReady, "end is not called")
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
    require(_isReady, "end is not called")
    s"""save ${_mode} ${_from} as ${_format.get}.`${_path.getOrElse("")}` ${_options.toFragment};"""
  }
}

class Python(parent: Byzer) extends BaseNode {

  private var _isReady = false
  private val _autogenTableName = UUID.randomUUID().toString.replaceAll("-", "")
  private var _tableName = _autogenTableName


  private var _options = new Options(this)

  private var _input = "command"
  private var _cache = true
  private var _confTable: Option[String] = None
  private var _model: Option[String] = None
  private var _schema: Option[String] = None
  private var _env: Option[String] = None
  private var _dataMode = "model"
  private var _runIn = "driver"
  private var _code: Option[String] = None
  private var _rawCode: Option[String] = None

  override def getTag: Option[String] = _tag

  def input(v: String) = {
    _input = v
    this
  }

  def output(v: String) = {
    _tableName = v
    this
  }

  def cache(v: Boolean) = {
    _cache = v
    this
  }

  def confTable(v: String) = {
    _confTable = Some(v)
    this
  }

  def model(v: String) = {
    _model = Some(v)
    this
  }

  def schema(v: String) = {
    _schema = Some(v)
    this
  }

  def env(v: String) = {
    _env = Some(v)
    this
  }

  def dataMode(v: String) = {
    _dataMode = v
    this
  }

  def runIn(v: String) = {
    _runIn = v
    this
  }

  def code(v: String) = {
    _code = Some(v)
    this
  }

  def codeWithHint(v: String) = {
    _rawCode = Some(v)
    this
  }

  override def tableName: String = {
    require(_isReady, "end is not called")
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
    require(_isReady, "end is not called")

    def confTableStr = {
      if (_confTable.isDefined) {
        s"""#%confTable=${_confTable.get}"""
      } else {
        ""
      }
    }

    def modelStr = {
      if (_model.isDefined) {
        s"""#%model=${_model.get}"""
      } else {
        ""
      }
    }

    def schemaStr = {
      if (_schema.isDefined) {
        s"""#%schema=${_schema.get}"""
      } else {
        ""
      }
    }

    def envStr = {
      if (_env.isDefined) {
        s"""#%env=${_env.get}"""
      } else {
        ""
      }
    }

    val finalCode = if (_rawCode.isDefined) {
      _rawCode.get
    } else {
      s"""#%python
         |#%input=${_input}
         |#%output=${_tableName}
         |#%cache=${_cache.toString}
         |${confTableStr}
         |${modelStr}
         |${schemaStr}
         |${envStr}
         |#%dataMode=${_dataMode}
         |#%runIn=${_runIn}
         |${_code.get}
         |""".stripMargin
    }
    val ph = new PythonHint
    ph.rewrite(finalCode, Map())
  }
}

sealed abstract class JoinType {
  def sql: String
}

/**
 * The explicitCartesian flag indicates if the inner join was constructed with a CROSS join
 * indicating a cartesian product has been explicitly requested.
 */
sealed abstract class InnerLike extends JoinType {
  def explicitCartesian: Boolean
}

case object Inner extends InnerLike {
  override def explicitCartesian: Boolean = false

  override def sql: String = "INNER"
}

case object Cross extends InnerLike {
  override def explicitCartesian: Boolean = true

  override def sql: String = "CROSS"
}

case object LeftOuter extends JoinType {
  override def sql: String = "LEFT OUTER"
}

case object RightOuter extends JoinType {
  override def sql: String = "RIGHT OUTER"
}

case object FullOuter extends JoinType {
  override def sql: String = "FULL OUTER"
}

case object LeftSemi extends JoinType {
  override def sql: String = "LEFT SEMI"
}

case object LeftAnti extends JoinType {
  override def sql: String = "LEFT ANTI"
}

case class NaturalJoin(tpe: JoinType) extends JoinType {
  require(Seq(Inner, LeftOuter, RightOuter, FullOuter).contains(tpe),
    "Unsupported natural join type " + tpe)

  override def sql: String = "NATURAL " + tpe.sql
}

case class UsingJoin(tpe: JoinType, usingColumns: Seq[String]) extends JoinType {
  require(Seq(Inner, LeftOuter, LeftSemi, RightOuter, FullOuter, LeftAnti, Cross).contains(tpe),
    "Unsupported using join type " + tpe)

  override def sql: String = "USING " + tpe.sql
}

object LeftSemiOrAnti {
  def unapply(joinType: JoinType): Option[JoinType] = joinType match {
    case LeftSemi | LeftAnti => Some(joinType)
    case _ => None
  }
}
