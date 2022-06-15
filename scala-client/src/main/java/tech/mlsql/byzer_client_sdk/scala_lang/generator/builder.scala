package tech.mlsql.byzer_client_sdk.scala_lang.generator

import net.sf.json.{JSONArray, JSONObject}
import tech.mlsql.byzer_client_sdk.scala_lang.generator.hint.PythonHint
import tech.mlsql.common.utils.serder.json.JSONTool

import java.util.UUID
import scala.collection.JavaConverters._
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
  private var blocks = new ArrayBuffer[BaseNode]()
  private var _cluster = new Cluster(this)

  def toJson(pretty: Boolean = false) = {
    val obj = new JSONObject()
    obj.put("version", 1.0)
    val arr = new JSONArray()
    blocks.map(_.toJson).map(item => JSONObject.fromObject(item)).foreach(item => arr.add(item))
    obj.put("blocks", arr)
    obj.put("cluster", JSONObject.fromObject(_cluster.toJson))
    if (pretty) {
      obj.toString(4)
    }
    else {
      obj.toString
    }
  }

  def fromJson(s: String): Byzer = {
    val obj = JSONObject.fromObject(s)
    obj.getJSONArray("blocks").asScala.foreach { item =>
      val temp = item.asInstanceOf[JSONObject]
      val block = Class.forName(temp.getJSONObject("__meta").getString("name")).
        getConstructor(classOf[Byzer]).
        newInstance(this).asInstanceOf[BaseNode]
      block.fromJson(temp.toString)
      blocks += block
    }
    _cluster = _cluster.fromJson(obj.getJSONObject("cluster").toString)
    this
  }

  def removeByTag(name: String) = {
    blocks = blocks.dropWhile(item => item.getTag.isDefined && item.getTag.get == name)
    this
  }

  def swapBlock(a: BaseNode, b: BaseNode) = {
    val newBlocks = ArrayBuffer[BaseNode]()

    val acIndex = blocks.indexOf(a)
    val bcIndex = blocks.indexOf(b)

    blocks.zipWithIndex.foreach { item =>
      if (item._2 == acIndex) {
        newBlocks.insert(item._2, b)
      } else if (item._2 == bcIndex) {
        newBlocks.insert(item._2, a)
      } else {
        newBlocks.insert(item._2, item._1)
      }
    }
    blocks = newBlocks
    this
  }

  def getByTag(name: String): List[BaseNode] = {
    blocks.filter(_.getTag.isDefined).filter(_.getTag.get == name).toList
  }

  def getUntilTag(name: String): List[BaseNode] = {
    blocks.zipWithIndex.filter(_._1.getTag.isDefined).filter(_._1.getTag.get == name).headOption match {
      case Some(item) => blocks.slice(0, item._2 + 1).toList
      case None => List()
    }
  }

  def run() = {
    _cluster.getMatchedEngines.map { engine =>
      engine.run()
    }
  }

  def runUntilTag(name: String) = {
    _cluster.getMatchedEngines.map { engine =>
      engine.runUntilTag(name)
    }
  }

  def runWithTag(name: String) = {
    _cluster.getMatchedEngines.map { engine =>
      engine.runWithTag(name)
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

  def toJson: String

  def fromJson(json: String): BaseNode
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

case class MetaMeta(name: String)

case class LoadMeta(__meta: MetaMeta,
                    _tag: Option[String],
                    _isReady: Boolean,
                    _autogenTableName: String,
                    _tableName: String,
                    _format: Option[String],
                    _path: Option[String],
                    _options: Map[String, OptionValue]
                   )

class Load(parent: Byzer) extends BaseNode {

  private var _isReady = false
  private var _autogenTableName = UUID.randomUUID().toString.replaceAll("-", "")
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

  override def fromJson(json: String): BaseNode = {
    val v = JSONTool.parseJson[LoadMeta](json)
    _tag = v._tag
    _isReady = v._isReady
    _autogenTableName = v._autogenTableName
    _tableName = v._tableName
    _format = v._format
    _path = v._path
    _options = new Options(this)
    v._options.foreach { item =>
      _options.addWithQuotedStr(item._1, item._2)
    }
    this
  }

  override def toJson: String = {
    JSONTool.toJsonStr(LoadMeta(
      __meta = MetaMeta(getClass.getName),
      _tag = _tag,
      _isReady = _isReady,
      _autogenTableName = _autogenTableName,
      _tableName = _tableName,
      _format = _format,
      _path = _path,
      _options = _options.items))
  }
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

trait FilterNodeMeta {}

case class AndFilterNodeMeta(k: String, left: FilterNodeMeta, right: FilterNodeMeta) extends FilterNodeMeta

case class OrFilterNodeMeta(k: String, left: FilterNodeMeta, right: FilterNodeMeta) extends FilterNodeMeta

case class ExprFilterNodeMeta(k: String, v: Expr) extends FilterNodeMeta

case class And(left: FilterNode, right: FilterNode) extends FilterNode

case class Or(left: FilterNode, right: FilterNode) extends FilterNode

case class Expr(expr: Option[String]) extends FilterNode

trait BaseOpt {
  def toFilterNode: FilterNode
}

case class AndOrOptMeta(__meta: MetaMeta, _clauses: List[FilterNodeMeta])


class AndOpt(parent: Filter) extends BaseOpt {

  private var _isReady = false
  private[generator] val _clauses = ArrayBuffer[FilterNode]()


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
  private[generator] var _clauses = ArrayBuffer[FilterNode]()


  def add(filterNode: FilterNode) = {
    _clauses += filterNode
    this
  }

  def tableName: String = {
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

case class FilterMeta(__meta: MetaMeta, _tag: Option[String], _isReady: Boolean, _autogenTableName: String,
                      _tableName: String, _from: String,
                      _clauses: List[AndOrOptMeta])

class Filter(parent: Byzer) extends BaseNode {
  private var _isReady = false
  private var _autogenTableName = UUID.randomUUID().toString.replaceAll("-", "")
  private var _tableName = _autogenTableName
  private var _clauses = ArrayBuffer[BaseOpt]()
  private var _from = parent.lastTableName

  private def toFilterNode(json: JSONObject): FilterNode = {

    json.getString("k") match {
      case "or" =>
        val left = json.getJSONObject("left")
        val right = json.getJSONObject("right")
        Or(toFilterNode(left), toFilterNode(right))
      case "and" =>
        val left = json.getJSONObject("left")
        val right = json.getJSONObject("right")
        And(toFilterNode(left), toFilterNode(right))
      case "expr" =>
        val v = json.getJSONObject("v")
        Expr(Some(v.getString("expr")))
    }
  }

  private def toFilterNodeMeta(v: FilterNode): FilterNodeMeta = {
    v match {
      case _ if v.isInstanceOf[And] =>
        val vv = v.asInstanceOf[And]
        AndFilterNodeMeta("and", toFilterNodeMeta(vv.left), toFilterNodeMeta(vv.right))
      case _ if v.isInstanceOf[Or] =>
        val vv = v.asInstanceOf[Or]
        OrFilterNodeMeta("or", toFilterNodeMeta(vv.left), toFilterNodeMeta(vv.right))
      case _ if v.isInstanceOf[Expr] =>
        ExprFilterNodeMeta("expr", v.asInstanceOf[Expr])
    }
  }

  override def fromJson(json: String): BaseNode = {
    val obj = JSONObject.fromObject(json)
    val clauseTemp = obj.remove("_clauses")

    clauseTemp.asInstanceOf[JSONArray].asScala.foreach { item =>
      val t = item.asInstanceOf[JSONObject]
      val opt = Class.forName(t.getJSONObject("__meta").getString("name")).
        getConstructor(classOf[Filter]).
        newInstance(this).asInstanceOf[BaseOpt]
      opt match {
        case _ if opt.isInstanceOf[AndOpt] =>
          val andOpt = opt.asInstanceOf[AndOpt]
          t.getJSONArray("_clauses").asScala.map { t =>
            andOpt.add(toFilterNode(t.asInstanceOf[JSONObject]))
          }
        case _ if opt.isInstanceOf[OrOpt] =>
          val orOpt = opt.asInstanceOf[AndOpt]
          t.getJSONArray("_clauses").asScala.map { t =>
            orOpt.add(toFilterNode(t.asInstanceOf[JSONObject]))
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
        case _ if item.isInstanceOf[AndOpt] =>
          AndOrOptMeta(MetaMeta(classOf[AndOpt].getName), item.asInstanceOf[AndOpt]._clauses.map { t =>
            toFilterNodeMeta(t)
          }.toList)
        case _ if item.isInstanceOf[OrOpt] =>
          AndOrOptMeta(MetaMeta(classOf[OrOpt].getName), item.asInstanceOf[OrOpt]._clauses.map { t =>
            toFilterNodeMeta(t)
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
case class JoinMeta(__meta: MetaMeta, _tag: Option[String], _isReady: Boolean, _autogenTableName: String,
                    _tableName: String, _from: String,
                    _joinTable: Option[String],
                    _on: Option[String],
                    _leftColumns: Option[String], _rightColumns: Option[String])

class Join(parent: Byzer) extends BaseNode {

  private var _isReady = false
  private var _autogenTableName = UUID.randomUUID().toString.replaceAll("-", "")
  private var _tableName = _autogenTableName

  private var _from = parent.lastTableName

  private var _joinTable: Option[String] = None
  private var _on: Option[String] = None
  private var _leftColumns: Option[String] = None
  private var _rightColumns: Option[String] = None

  override def fromJson(json: String): BaseNode = {
    val v = JSONTool.parseJson[JoinMeta](json)
    _tag = v._tag
    _isReady = v._isReady
    _autogenTableName = v._autogenTableName
    _tableName = v._tableName
    _from = v._from
    _joinTable = v._joinTable
    _on = v._on
    _leftColumns = v._leftColumns
    _rightColumns = v._rightColumns
    this
  }

  override def toJson: String = {
    JSONTool.toJsonStr(JoinMeta(
      __meta = MetaMeta(getClass.getName),
      _tag = _tag,
      _isReady = _isReady,
      _autogenTableName = _autogenTableName,
      _tableName = _tableName,
      _from = _from,
      _joinTable = _joinTable,
      _on = _on,
      _leftColumns = _leftColumns,
      _rightColumns = _rightColumns,
    ))
  }

  def from(expr: Expr) = {
    _from = expr.toFragment
    this
  }

  override def tableName: String = {
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

case class RegisterMeta(__meta: MetaMeta, _tag: Option[String], _isReady: Boolean, _autogenTableName: String,
                        _tableName: String,
                        _options: Map[String, OptionValue],
                        _name: String,
                        _tpe: String,
                        _lang: String,
                        _code: Option[String]
                       )

class Register(parent: Byzer) extends BaseNode {
  private var _isReady = false
  private var _autogenTableName = UUID.randomUUID().toString.replaceAll("-", "")
  private var _tableName = _autogenTableName

  private[generator] var _options = new Options(this)
  private[generator] var _name = "EmptyTable"
  private[generator] var _tpe = "udf"
  private[generator] var _lang = "scala"
  private[generator] var _code: Option[String] = None

  override def fromJson(json: String): BaseNode = {
    val v = JSONTool.parseJson[RegisterMeta](json)
    _tag = v._tag
    _isReady = v._isReady
    _autogenTableName = v._autogenTableName
    _tableName = v._tableName
    _name = v._name
    _tpe = v._tpe
    _code = v._code
    _lang = v._lang

    _options = new Options(this)
    v._options.foreach { item =>
      _options.addWithQuotedStr(item._1, item._2)
    }
    this
  }

  override def toJson: String = {
    JSONTool.toJsonStr(RegisterMeta(
      __meta = MetaMeta(getClass.getName),
      _tag = _tag,
      _isReady = _isReady,
      _autogenTableName = _autogenTableName,
      _tableName = _tableName,
      _name = _name,
      _tpe = _tpe,
      _code = _code,
      _lang = _lang,
      _options = _options.items
    ))
  }

  def udf() = {
    new UDF(this)
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
    s"""register ScriptUDF.`` as ${_name}
       |where lang="${_lang}"
       |and code='''${_code.get}'''
       |and udfType="${_tpe}";""".stripMargin
  }
}

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

case class PythonMeta(__meta: MetaMeta, _tag: Option[String], _isReady: Boolean, _autogenTableName: String,
                      _tableName: String,
                      _options: Map[String, OptionValue],
                      _input: String,
                      _cache: Boolean,
                      _confTable: Option[String],
                      _model: Option[String],
                      _schema: Option[String],
                      _env: Option[String],
                      _dataMode: String,
                      _runIn: String,
                      _code: Option[String],
                      _rawCode: Option[String]

                     )

class Python(parent: Byzer) extends BaseNode {

  private var _isReady = false
  private var _autogenTableName = UUID.randomUUID().toString.replaceAll("-", "")
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

  override def fromJson(json: String): BaseNode = {
    val v = JSONTool.parseJson[PythonMeta](json)
    _tag = v._tag
    _isReady = v._isReady
    _autogenTableName = v._autogenTableName
    _tableName = v._tableName
    _input = v._input
    _cache = v._cache
    _confTable = v._confTable
    _model = v._model
    _schema = v._schema
    _env = v._env
    _dataMode = v._dataMode
    _runIn = v._runIn
    _code = v._code
    _rawCode = v._rawCode
    _options = new Options(this)
    v._options.foreach { item =>
      _options.addWithQuotedStr(item._1, item._2)
    }
    this
  }

  override def toJson: String = {
    JSONTool.toJsonStr(PythonMeta(
      __meta = MetaMeta(getClass.getName),
      _tag = _tag,
      _isReady = _isReady,
      _autogenTableName = _autogenTableName,
      _tableName = _tableName,
      _input = _input,
      _cache = _cache,
      _confTable = _confTable,
      _model = _model,
      _schema = _schema,
      _env = _env,
      _dataMode = _dataMode,
      _runIn = _runIn,
      _code = _code,
      _rawCode = _rawCode,
      _options = _options.items
    ))
  }

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
