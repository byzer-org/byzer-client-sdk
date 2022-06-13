package tech.mlsql.test.byzer_client_sdk.scala_lang.generator

import org.scalatest.funsuite.AnyFunSuite
import tech.mlsql.byzer_client_sdk.scala_lang.generator.{And, Byzer, Expr, LangScalaType, UDFUDFType}

/**
 * 10/6/2022 WilliamZhu(allwefantasy@gmail.com)
 */
class ByzerScriptTest extends AnyFunSuite {

  /**
   * load csv.`/tmp/jack` where `header`='''true''' as 004f7b1361904755a223a543c613a387;
   */
  test("load") {
    val genCode = Byzer().load.format("csv").path("/tmp/jack").
      options().add("header", "true").end.
      end.toScript
    println(genCode)
  }

  /**
   * load csv.`/tmp/jack` where `header`='''true''' as 20e4e3bd09b448f4b20b412fab5a7c56;
   * select * from 20e4e3bd09b448f4b20b412fab5a7c56 where (a=b or (c>2 and d>10)) as f82f555639044b46afbc7231efeb37c4;
   */
  test("filter") {
    val genCode = Byzer().
      load.format("csv").path("/tmp/jack").options().add("header", "true").end.end.
      filter.or.add(Expr(Some("a=b"))).add(And(Expr(Some("c>2")), Expr(Some("d>10")))).end.end.
      toScript
    println(genCode)
  }

  /**
   * load csv.`/tmp/jack` where `header`='''true''' as f4bf0ec5a600487a892980f83203ddf8;
   * select * from f4bf0ec5a600487a892980f83203ddf8 where (a=b or (c>2 and d>10)) as 1627d9aad8a2428c815d6b2a1e92e129;
   * select split('a',',')[0] as a2 from 1627d9aad8a2428c815d6b2a1e92e129 as e633932623014c09ade71b62d618e212;
   */
  test("project") {
    val genCode = Byzer().
      load.format("csv").path("/tmp/jack").options().add("header", "true").end.end.
      filter.or.add(Expr(Some("a=b"))).add(And(Expr(Some("c>2")), Expr(Some("d>10")))).end.end.
      columns.addColumn(Expr(Some("split('a',',')[0] as a2"))).end.
      toScript

    println(genCode)
  }

  /**
   * load csv.`/tmp/jack` where `header`='''true''' as table1;
   * select * from table1 where (a=b or (c>2 and d>10)) as 271a2df28abb469da3ee87566873fdbd;
   * select split('a',',')[0] as a2 from 271a2df28abb469da3ee87566873fdbd as a3ab7cc1682f4f11935cbfa3281bf454;
   */
  test("project with specific table name") {
    val genCode = Byzer().
      load.format("csv").path("/tmp/jack").options().add("header", "true").end.namedTableName("table1").end.
      filter.from("table1").or.add(Expr(Some("a=b"))).add(And(Expr(Some("c>2")), Expr(Some("d>10")))).end.end.
      columns.addColumn(Expr(Some("split('a',',')[0] as a2"))).end.
      toScript

    println(genCode)
  }

  /**
   * load csv.`/tmp/jack` where `header`='''true''' as e7a9b351064b4cd399b819defbfbf5f0;
   * load csv.`/tmp/william` where `header`='''true''' as 8c6f1d82e75a4d61baf31924c55c845a;
   * select e7a9b351064b4cd399b819defbfbf5f0.a,e7a9b351064b4cd399b819defbfbf5f0.c,8c6f1d82e75a4d61baf31924c55c845a.m
   * from e7a9b351064b4cd399b819defbfbf5f0 LEFT OUTER 8c6f1d82e75a4d61baf31924c55c845a
   * on e7a9b351064b4cd399b819defbfbf5f0.a=8c6f1d82e75a4d61baf31924c55c845a.b;
   */
  test("join") {
    val byzer = Byzer()
    val table1 = byzer.load.format("csv").path("/tmp/jack").options().add("header", "true").end
    val table2 = byzer.load.format("csv").path("/tmp/william").options().add("header", "true").end
    table1.end
    table2.end

    val t1 = table1.tableName
    val t2 = table2.tableName

    val genCode = byzer.join.
      from(Expr(Some(t1))).
      left(Expr(Some(t2))).
      on(Expr(Some(s"""${t1}.a=${t2}.b"""))).
      leftColumns(Expr(Some(s"${t1}.a,${t1}.c"))).
      rightColumns(Expr(Some(s"""${t2}.m"""))).end.toScript

    println(genCode)
  }

  /**
   * load csv.`/tmp/jack` where `header`='''true''' as a781f5ca79f4493e9ba887cd89efecf8;
   * train a781f5ca79f4493e9ba887cd89efecf8 as RandomForest.``
   * where `fit.0.maxDept`='''3''' as 9322b7fed5394700a7dc3b2ece67eaf9;
   */
  test("mod") {
    val byzer = Byzer()
    val table1 = byzer.load.format("csv").path("/tmp/jack").options().add("header", "true").end
    table1.end
    val t1 = table1.tableName

    val genCode = byzer.mod.train.
      from(Expr(Some(t1))).name(Expr(Some("RandomForest"))).
      options.add("fit.0.maxDept", "3").end.end.toScript

    println(genCode)
  }

  /**
   * load csv.`/tmp/jack` where `header`='''true''' as c68c53d70c6e4921aeeee9abb15190ff;
   * save append c68c53d70c6e4921aeeee9abb15190ff as parquet.`/tmp/william`;
   */
  test("save") {
    val byzer = Byzer()
    val table1 = byzer.load.format("csv").path("/tmp/jack").options().add("header", "true").end
    table1.end
    val t1 = table1.tableName

    val genCode = byzer.save.from(t1).format("parquet").path("/tmp/william").end.toScript
    println(genCode)
  }

  /**
   * include lib.`gitee.com/allwefantasy/lib-core` where `alias`='''libCore''' and `forceUpdate`='''false''';
   * include local.`libCore.udf.hello` ;
   */
  test("include") {
    val byzer = Byzer()
    val genCode = byzer.include.lib("gitee.com/allwefantasy/lib-core").alias("libCore").end.end.
      include.`package`("libCore.udf.hello").end.end.
      toScript
    println(genCode)
  }

  /**
   * register ScriptUDF.`` as arrayLast
   * where lang="scala"
   * and code='''
   * def apply(a:Seq[String])={
   *       a.last
   * }
   * '''
   * and udfType="udf";
   */
  test("register-udf") {
    val byzer = Byzer()
    val genCode = byzer.register.udf.tpe(UDFUDFType).lang(LangScalaType).code(
      """
        |def apply(a:Seq[String])={
        |      a.last
        |}
        |""".stripMargin).name("arrayLast").end.end.toScript
    println(genCode)
  }

}
