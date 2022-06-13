package tech.mlsql.byzer_client_sdk.scala_lang.generator

import org.apache.http.client.fluent.{Form, Request}
import tech.mlsql.byzer_client_sdk.scala_lang.generator.Cluster.executor
import tech.mlsql.common.utils.distribute.socket.server.JavaUtils
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.common.utils.serder.json.JSONTool

import java.nio.charset.Charset
import java.util.UUID
import java.util.concurrent.{Executors, TimeUnit}
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object Cluster {
  private[generator] val executor = Executors.newSingleThreadScheduledExecutor()
}

class Cluster(parent: Byzer) {
  private[generator] val _clusters = ArrayBuffer[Engine]()
  private[generator] var _refreshTime = 1l
  private[generator] var _backendStrategy: Option[BackendStrategy] = None

  def engine = {
    val engine = new Engine(parent, this)
    _clusters += engine
    engine
  }

  def getMatchedEngines: Seq[Engine] = {
    if (_backendStrategy.isEmpty) {
      return _backendStrategy.get.invoke(_clusters).getOrElse(Seq())
    }
    _clusters
  }

  def backendStrategy(s: BackendStrategy) = {
    _backendStrategy = Some(s)
    this
  }

  def refreshMetaTime(s: String) = {
    _refreshTime = JavaUtils.timeStringAsSec(s)
    this
  }

  def end = {
    executor.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = {
        _clusters.foreach { engine =>
          val resource = engine.showResource
          engine._meta = EngineMeta(resource.activeTasks, resource.totalCores)
        }
      }
    }, 1, _refreshTime, TimeUnit.SECONDS)
    parent
  }
}

case class EngineMeta(activeTaskNum: Long, totalCores: Long)


class Engine(parent: Byzer, cluster: Cluster) extends Logging {
  private var _url = ""
  private var _params: Map[String, String] = Map()
  private[generator] var _tag: Option[scala.collection.Set[String]] = None

  private[generator] var _meta = EngineMeta(-1, -1)

  def url(s: String) = {
    _url = s
    this
  }

  private val extraParams = mutable.HashMap[String, String]()
  extraParams.put("executeMode", "query")
  extraParams.put("sessionPerUser", "true")
  extraParams.put("sessionPerRequest", "true")
  extraParams.put("includeSchema", "true")
  extraParams.put("fetchType", "take")


  def includeSchema(include: Boolean) = {
    extraParams += ("includeSchema" -> include.toString)
    this
  }

  def fetchType(fetchTpe: Boolean) = {
    extraParams += ("fetchType" -> fetchTpe.toString)
    this
  }

  def sql(sql: String) = {
    extraParams += ("sql" -> sql)
    this
  }

  def owner(owner: String) = {
    extraParams += ("owner" -> owner)
    this
  }

  def async(async: Boolean) = {
    extraParams += ("async" -> async.toString)
    this
  }

  def timeout(timeout: Long) = {
    extraParams += ("timeout" -> timeout.toString)
    this
  }

  def executeMode(executeMode: String) = {
    extraParams += ("executeMode" -> executeMode)
    this
  }

  def jobName(jobName: String) = {
    extraParams += ("jobName" -> jobName)
    this
  }

  def showResource: InstanceResource = {
    try {
      val res = runSQL("!show resource").returnContent().asString()
      val resource = JSONTool.parseJson[InstanceResource](res)
      resource
    } catch {
      case e: Exception =>
        logError(s"Engine(${_url}) fails", e)
        InstanceResource(-1, -1, -1, -1, -1)
    }
  }


  private def param(str: String) = {
    params().getOrElse(str, null)
  }

  private def hasParam(str: String) = {
    params().contains(str)
  }

  private def params() = {
    _params ++ extraParams
  }


  def runUntilTag(name: String) = {
    sql(parent.getUntilTag(name).map(item => item.toBlock).mkString("\n")).execute()
  }

  def runWithTag(name: String) = {
    sql(parent.getByTag(name).map(item => item.toBlock).mkString("\n")).execute()
  }

  def run() = {
    sql(parent.toScript).execute()
  }

  def runSQL(sql: String) = {
    var newparams = params()

    if (!newparams.contains("jobName")) {
      newparams += ("jobName" -> UUID.randomUUID().toString)
    }

    val form = Form.form()
    (newparams ++ Map("sql" -> sql)).foreach { case (k, v) =>
      form.add(k, v)
    }

    val content = Request.Post(_url).
      bodyForm(form.build(), Charset.forName("utf-8")).
      execute()

    content
  }

  private def execute() = {

    var newparams = params()

    if (!newparams.contains("jobName")) {
      newparams += ("jobName" -> UUID.randomUUID().toString)
    }

    val form = Form.form()
    newparams.foreach { case (k, v) =>
      form.add(k, v)
    }

    val content = Request.Post(_url).
      bodyForm(form.build(), Charset.forName("utf-8")).
      execute()

    content

  }

  def end = {
    cluster
  }
}

trait BackendStrategy {
  def invoke(backends: Seq[Engine]): Option[Seq[Engine]]
}

class AllBackendsStrategy(tags: String) extends BackendStrategy {
  override def invoke(backends: Seq[Engine]): Option[Seq[Engine]] = {
    val tagSet = tags.split(",").toSet
    if (tags.isEmpty) {
      Option(backends)
    } else {
      Option(backends.filter(f => tagSet.intersect(f._tag.get).size > 0))
    }

  }
}

class JobNumAwareStrategy(tags: String) extends BackendStrategy {
  override def invoke(backends: Seq[Engine]): Option[Seq[Engine]] = {
    val tagSet = tags.split(",").toSet
    var targetBackends = backends.filter(_._tag.isDefined)
    targetBackends = if (tagSet.size > 0) {
      targetBackends.filter(_._tag.get.intersect(tagSet).size > 0).sortBy(_._meta.activeTaskNum)
    } else {
      targetBackends.sortBy(_._meta.activeTaskNum)
    }
    if (targetBackends.size == 0) {
      return None
    }
    Option(Seq(targetBackends.head))
  }
}

class ResourceAwareStrategy(tags: String) extends BackendStrategy with Logging {
  override def invoke(backends: Seq[Engine]): Option[Seq[Engine]] = {

    val tagSet = tags.split(",").toSet

    var targetBackends = backends
    if (!tags.isEmpty) {
      targetBackends = targetBackends.filter(_._tag.get.intersect(tagSet).size > 0)
    }
    if (targetBackends.size == 0) {
      return None
    }
    targetBackends = targetBackends.map { b =>
      var returnItem = (Long.MaxValue, b)
      returnItem = (b._meta.totalCores - b._meta.activeTaskNum, b)
      returnItem
    }.sortBy(f => f._1).reverse.map(_._2)
    Option(targetBackends)
  }
}

case class InstanceResource(activeTasks: Long, totalCores: Long, totalTasks: Long, totalUsedMemory: Long, totalMemory: Long)
