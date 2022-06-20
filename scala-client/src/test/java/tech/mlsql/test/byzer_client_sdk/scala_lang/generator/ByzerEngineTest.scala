package tech.mlsql.test.byzer_client_sdk.scala_lang.generator

import org.scalatest.funsuite.AnyFunSuite
import tech.mlsql.byzer_client_sdk.scala_lang.generator.{Byzer, Expr, ResourceAwareStrategy}

/**
 * 13/6/2022 WilliamZhu(allwefantasy@gmail.com)
 */
class ByzerEngineTest extends AnyFunSuite {
  test("engine") {
    val byzer = Byzer().cluster().
      engine.url("http://127.0.0.1:9004/run/script").owner("admin").end.
      backendStrategy(new ResourceAwareStrategy("")).
      end
    val script = byzer.variable.name(Expr(Some("data"))).value(Expr(Some(
      """
        |{ "x": 100, "y": 200, "z": 200 ,"dataType":"A group"}
        |{ "x": 120, "y": 100, "z": 260 ,"dataType":"B group"}
        |""".stripMargin))).end.load.format("jsonStr").path("data").namedTableName("table1").tag("load_json").end.
      columns.addColumn(Expr(Some("x"))).end

    println(script.commands.schema)
    println(byzer.commands.showDataSourceParam("csv"))
    println(byzer.commands.showETs)

//    val res = script.run()
//    println(res.head.returnContent().asString())
//
//    val res1 = script.runUntilTag("load_json")
//    println(res1.head.returnContent().asString())
//
//    val res2 = script.runWithTag("load_json")
//    println(res2.head.returnContent().asString())
  }

}
