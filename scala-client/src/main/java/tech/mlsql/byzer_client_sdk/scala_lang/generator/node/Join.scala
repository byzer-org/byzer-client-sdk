package tech.mlsql.byzer_client_sdk.scala_lang.generator.node

import net.sf.json.{JSONArray, JSONObject}
import tech.mlsql.byzer_client_sdk.scala_lang.generator._
import tech.mlsql.common.utils.serder.json.JSONTool

import java.util.UUID
import scala.::
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

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

  override def sql: String = "INNER JOIN"
}

case object Cross extends InnerLike {
  override def explicitCartesian: Boolean = true

  override def sql: String = "CROSS JOIN"
}

case object LeftOuter extends JoinType {
  override def sql: String = "LEFT OUTER JOIN"
}

case object RightOuter extends JoinType {
  override def sql: String = "RIGHT OUTER JOIN"
}

case object FullOuter extends JoinType {
  override def sql: String = "FULL OUTER JOIN"
}

case object LeftSemi extends JoinType {
  override def sql: String = "LEFT SEMI JOIN"
}

case object LeftAnti extends JoinType {
  override def sql: String = "LEFT ANTI JOIN"
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

case class JoinMeta(__meta: MetaMeta, _tag: Option[String], _isReady: Boolean, _autogenTableName: String,
                    _tableName: String, _from: String,
                    _joinTable: List[Option[String]],
                    _on:Option[List[AndOrOptMeta]],
                    _leftColumns: Option[String], _rightColumns: List[Option[String]])

/**
 * Byzer().join.from(...).left(...).on(...).leftColumns(....).rightColumns(.....)
 */
class Join(parent: Byzer) extends BaseNode {

  private var _isReady = false
  private var _autogenTableName = UUID.randomUUID().toString.replaceAll("-", "")
  private var _tableName = _autogenTableName

  private var _from = parent.lastTableName

  private var _joinTable = new ArrayBuffer[Option[String]]()
  private var _on = ArrayBuffer[BaseOpt]()

  private var _leftColumns: Option[String] = None
  private var _rightColumns = new ArrayBuffer[Option[String]]()

  override def fromJson(json: String): BaseNode = {

    val obj = JSONObject.fromObject(json)
    val onClauseTemp = obj.remove("_on")

    onClauseTemp.asInstanceOf[JSONArray].asScala.foreach { item =>
      val t = item.asInstanceOf[JSONObject]
      val opt = Class.forName(t.getJSONObject("__meta").getString("name")).
        getConstructor(classOf[BaseNode]).
        newInstance(this).asInstanceOf[BaseOpt]
      opt match {
        case _ if opt.isInstanceOf[AndOpt[Join]] =>
          val andOpt = opt.asInstanceOf[AndOpt[Join]]
          t.getJSONArray("_clauses").asScala.map { t =>
            andOpt.add(AndOrTools.toFilterNode(t.asInstanceOf[JSONObject]))
          }
        case _ if opt.isInstanceOf[OrOpt[Join]] =>
          val orOpt = opt.asInstanceOf[AndOpt[Join]]
          t.getJSONArray("_clauses").asScala.map { t =>
            orOpt.add(AndOrTools.toFilterNode(t.asInstanceOf[JSONObject]))
          }
      }
      _on += opt
    }

    val joinTableTemp = obj.remove("_joinTable")
    joinTableTemp.asInstanceOf[JSONArray].asScala.foreach { item =>
      val itemStr = item.asInstanceOf[String]
      _joinTable += Some(itemStr)
    }

    val rightColumnsTemp = obj.remove("_rightColumns")
    joinTableTemp.asInstanceOf[JSONArray].asScala.foreach { item =>
      val itemStr = item.asInstanceOf[String]
      _rightColumns += Some(itemStr)
    }

    val v = JSONTool.parseJson[JoinMeta](obj.toString)
    _tag = v._tag
    _isReady = v._isReady
    _autogenTableName = v._autogenTableName
    _leftColumns = v._leftColumns
    _tableName = v._tableName
    _from = v._from
    this
  }

  override def toJson: String = {

    val onClauses = _on.map { item =>
      item match {
        case _ if item.isInstanceOf[AndOpt[Join]] =>
          AndOrOptMeta(MetaMeta(classOf[AndOpt[Join]].getName), item.asInstanceOf[AndOpt[Join]]._clauses.map { t =>
            AndOrTools.toFilterNodeMeta(t)
          }.toList)
        case _ if item.isInstanceOf[OrOpt[Join]] =>
          AndOrOptMeta(MetaMeta(classOf[OrOpt[Join]].getName), item.asInstanceOf[OrOpt[Join]]._clauses.map { t =>
            AndOrTools.toFilterNodeMeta(t)
          }.toList)
      }
    }

    JSONTool.toJsonStr(JoinMeta(
      __meta = MetaMeta(getClass.getName),
      _tag = _tag,
      _isReady = _isReady,
      _autogenTableName = _autogenTableName,
      _tableName = _tableName,
      _from = _from,
      _joinTable = _joinTable.toList,
      _on = Some(onClauses.toList),
      _leftColumns = _leftColumns,
      _rightColumns = _rightColumns.toList,
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
    require(_isReady, "end is not called")
    if (_joinTable.size != _on.size) {
      throw new RuntimeException(s"${_joinTable.size} join() but ${_on.size} on(), " +
        s"the join method must correspond to the on method one by one!")
    }

    var joinClause: ArrayBuffer[String] = new ArrayBuffer[String]()
    for( flag <- _joinTable.indices) {
      val joinTable = _joinTable(flag).get
      val cla = _on(flag).toFilterNode.toFragment
      joinClause += s"${joinTable} on ${cla}"
    }

    s"""select ${_leftColumns.get},${_rightColumns.map(col=>col.get).mkString(",")}
    |from ${_from}
    |${joinClause.mkString("\n")}
    |as ${_tableName};""".stripMargin
  }


  def leftColumns(expr: Expr) = {
    _leftColumns = Some(expr.toFragment)
    this
  }

  def rightColumns(expr: Expr) = {
    _rightColumns += Some(expr.toFragment)
    this
  }

  def on_or(): OrOpt[Join] = {
    val n = new OrOpt(this)
    _on += n
    n
  }

  def on_and(): AndOpt[Join] = {
    val n = new AndOpt(this)
    _on += n
    n
  }

  def left(expr: Expr) = {
    _joinTable += Some(s"""${LeftOuter.sql} ${expr.toFragment}""")
    this
  }

  def right(expr: Expr) = {
    _joinTable += Some(s"""${RightOuter.sql} ${expr.toFragment}""")
    this
  }

  def inner(expr: Expr) = {
    _joinTable += Some(s"""${Inner.sql} ${expr.toFragment}""")
    this
  }

  def fullouter(expr: Expr) = {
    _joinTable += Some(s"""${FullOuter.sql} ${expr.toFragment}""")
    this
  }

  def leftsemi(expr: Expr) = {
    _joinTable += Some(s"""${LeftSemi.sql} ${expr.toFragment}""")
    this
  }

  def leftanti(expr: Expr) = {
    _joinTable += Some(s"""${LeftAnti.sql} ${expr.toFragment}""")
    this
  }

  def cross(expr: Expr) = {
    _joinTable += Some(s"""${Cross.sql} ${expr.toFragment}""")
    this
  }

}
