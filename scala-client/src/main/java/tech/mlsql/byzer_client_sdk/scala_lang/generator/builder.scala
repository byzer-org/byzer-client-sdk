package tech.mlsql.byzer_client_sdk.scala_lang.generator

import net.sf.json.JSONObject

import java.util.UUID
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

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

object AndOrTools {
  def toFilterNode(json: JSONObject): FilterNode = {

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

  def toFilterNodeMeta(v: FilterNode): FilterNodeMeta = {
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
}

case class AndOrOptMeta(__meta: MetaMeta, _clauses: List[FilterNodeMeta])


class AndOpt[T <: BaseNode](parent: T) extends BaseOpt {

  private var _isReady = false
  private[generator] val _clauses = ArrayBuffer[FilterNode]()


  def add(filterNode: FilterNode) = {
    _clauses += filterNode
    this
  }

  def end: T = {
    _isReady = true
    parent
  }

  override def toFilterNode: FilterNode = {
    val n = _clauses.reduce((a, b) => And(a, b))
    n
  }
}

class OrOpt[T <: BaseNode](parent: T) extends BaseOpt {

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

  def namedTableName(tableName: String): OrOpt[T] = {
    _tableName = tableName
    this
  }

  def desc(str: String): BaseNode = ???

  def end: T = {
    _isReady = true
    parent
  }

  override def toFilterNode: FilterNode = {
    val n = _clauses.reduce((a, b) => Or(a, b))
    n
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
