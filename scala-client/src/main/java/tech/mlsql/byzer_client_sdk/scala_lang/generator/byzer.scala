package tech.mlsql.byzer_client_sdk.scala_lang.generator

import net.sf.json.{JSONArray, JSONObject}
import tech.mlsql.common.utils.serder.json.JsonUtils

import scala.collection.JavaConverters._
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

class Commands(byzer: Byzer) {

  private def singleCheck = {
    require(byzer.blocks.isEmpty, "Please make sure there is no other script.")
  }

  def showResource = {
    singleCheck
    val r = _inner[Res[ResResource]]("!show resource;")
    r.data
  }

  private def _inner[T](command: String)(implicit mt: Manifest[T]) = {
    try {
      val str = byzer.raw.code(command).end.run().head.returnContent().asString()
      val res = JsonUtils.fromJson[T](str)
      res
    } finally {
      byzer.dropLast
    }
  }

  def showJobs = {
    singleCheck
    val r = _inner[Res[ResJobInfo]]("!show jobs;")
    r.data
  }


  def schema = {
    val tableName = byzer.lastTableName
    try {
      byzer.raw.code(s"!desc ${tableName};").end
      val s = byzer.run()
      val str = s.head.returnContent().asString()
      val res = JsonUtils.fromJson[Res[ResTableSchema]](str)
      res.data
    } finally {
      byzer.dropLast
    }
  }
}

class Byzer {
  private[generator] var blocks = new ArrayBuffer[BaseNode]()
  private[generator] var _cluster = new Cluster(this)


  def commands = {
    require(_cluster._clusters.length > 0, "engine should be configured in order to infer the schema")
    new Commands(this)
  }


  def dropLast = {
    blocks = blocks.dropRight(1)
    this
  }

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

  def order = {
    val block = new OrderBy(this)
    blocks += block
    block
  }

  def raw = {
    val block = new Raw(this)
    blocks += block
    block
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