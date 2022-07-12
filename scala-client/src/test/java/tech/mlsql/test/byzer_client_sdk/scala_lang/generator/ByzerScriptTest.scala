package tech.mlsql.test.byzer_client_sdk.scala_lang.generator

import org.scalatest.funsuite.AnyFunSuite
import tech.mlsql.byzer_client_sdk.scala_lang.generator._
import tech.mlsql.byzer_client_sdk.scala_lang.generator.node.{Columns, Filter, JoinMeta}
import tech.mlsql.common.utils.serder.json.JSONTool

/**
 * 10/6/2022 WilliamZhu(allwefantasy@gmail.com)
 */
class ByzerScriptTest extends AnyFunSuite {

  /**
   * load csv.`/tmp/jack` where `header`='''true''' as 004f7b1361904755a223a543c613a387;
   */
  test("load") {
    val byzer = Byzer()
    val genCode = byzer.load.format("csv").path("/tmp/jack").
      options().add("header", "true").end.tag("table1").
      end.toScript
    val t = byzer.getByTag("table1").head
    assert(genCode == s"""load csv.`/tmp/jack` where `header`='''true''' as ${t.tableName};""")
  }

  /**
   * load csv.`/tmp/jack` where `header`='''true''' as table1;
   * select * from table1 where (a=b or (c>2 and d>10)) as table2;
   */
  test("filter") {
    val genCode = Byzer().
      load.format("csv").path("/tmp/jack").options().add("header", "true").end.namedTableName("table1").end.
      filter.or.add(Expr(Some("a=b"))).add(And(Expr(Some("c>2")), Expr(Some("d>10")))).end.namedTableName("table2").end.
      toScript
    assert(genCode ==
      s"""load csv.`/tmp/jack` where `header`='''true''' as table1;
         |select * from table1 where (a=b or (c>2 and d>10)) as table2;""".stripMargin)
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
   * load csv.`/tmp/jack` where `header`='''true''' as ab2e364a24ea4f50b7ac8010772a67b8;
   * load csv.`/tmp/william` where `header`='''true''' as 649a1142023349c7b19f9b265855b8dd;
   * select ab2e364a24ea4f50b7ac8010772a67b8.a,ab2e364a24ea4f50b7ac8010772a67b8.c,649a1142023349c7b19f9b265855b8dd.m
   * from ab2e364a24ea4f50b7ac8010772a67b8 LEFT OUTER 649a1142023349c7b19f9b265855b8dd
   * on (ab2e364a24ea4f50b7ac8010772a67b8.a=649a1142023349c7b19f9b265855b8dd.b
   * and ab2e364a24ea4f50b7ac8010772a67b8.a=649a1142023349c7b19f9b265855b8dd.c)
   * as 73dfaeda7b3241bf8527fe073ea4d0e9;
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
      on_and.add(Expr(Some(s"""${t1}.a=${t2}.b"""))).add(Expr(Some(s"""${t1}.a=${t2}.c"""))).end.
      leftColumns(Expr(Some(s"${t1}.a,${t1}.c"))).
      rightColumns(Expr(Some(s"""${t2}.m"""))).end.toScript

    println(genCode)
  }

  test("join-multi") {
    val byzer = Byzer()
    val table1 = byzer.load.format("csv").path("/tmp/jack").options().add("header", "true").end
    val table2 = byzer.load.format("csv").path("/tmp/william").options().add("header", "true").end
    val table3 = byzer.load.format("csv").path("/tmp/admond").options().add("header", "true").end
    table1.namedTableName("table1").end
    table2.namedTableName("table2").end
    table3.namedTableName("table3").end

    val t1 = table1.tableName
    val t2 = table2.tableName
    val t3 = table3.tableName

    val genCode = byzer.join.
      from(Expr(Some(t1))).
      leftColumns(Expr(Some(s"${t1}.a,${t1}.c"))).
      rightColumns(Expr(Some(s"""${t2}.m,${t3}.n"""))).
      rightColumns(Expr(Some(s"""${t2}.g,${t3}.t"""))).
      left(Expr(Some(t2))).
      on_and.add(Expr(Some(s"""${t1}.a=${t2}.b"""))).add(Expr(Some(s"""${t1}.a=${t2}.c"""))).end.
      right(Expr(Some(t3))).
      on_or.add(Expr(Some(s"""${t1}.a=${t3}.d"""))).add(Expr(Some(s"""${t1}.a=${t3}.e"""))).add(Expr(Some(s"""${t1}.a=${t3}.f"""))).end.
      namedTableName("outputTable").end.toScript

    val target =
      """load csv.`/tmp/jack` where `header`='''true''' as table1;
        |load csv.`/tmp/william` where `header`='''true''' as table2;
        |load csv.`/tmp/admond` where `header`='''true''' as table3;
        |select table1.a,table1.c,table2.m,table3.n,table2.g,table3.t
        |from table1
        |LEFT OUTER JOIN table2 on (table1.a=table2.b and table1.a=table2.c)
        |RIGHT OUTER JOIN table3 on ((table1.a=table3.d or table1.a=table3.e) or table1.a=table3.f)
        |as outputTable;""".stripMargin
    assert(genCode == target, "Generate multi join clause error!")
  }

  test("join-multi-error") {
    val byzer = Byzer()
    val table1 = byzer.load.format("csv").path("/tmp/jack").options().add("header", "true").end
    val table2 = byzer.load.format("csv").path("/tmp/william").options().add("header", "true").end
    val table3 = byzer.load.format("csv").path("/tmp/admond").options().add("header", "true").end
    table1.namedTableName("table1").end
    table2.namedTableName("table2").end
    table3.namedTableName("table3").end

    val t1 = table1.tableName
    val t2 = table2.tableName
    val t3 = table3.tableName

    val caught =
      intercept[RuntimeException] {
        byzer.join.
          from(Expr(Some(t1))).
          leftColumns(Expr(Some(s"${t1}.a,${t1}.c"))).
          rightColumns(Expr(Some(s"""${t2}.m,${t3}.n"""))).
          rightColumns(Expr(Some(s"""${t2}.g,${t3}.t"""))).
          left(Expr(Some(t2))).
          on_and.add(Expr(Some(s"""${t1}.a=${t2}.b"""))).add(Expr(Some(s"""${t1}.a=${t2}.c"""))).end.
          right(Expr(Some(t3))).
          on_or.add(Expr(Some(s"""${t1}.a=${t3}.d"""))).add(Expr(Some(s"""${t1}.a=${t3}.e"""))).add(Expr(Some(s"""${t1}.a=${t3}.f"""))).end.
          left(Expr(Some(t2))).
          namedTableName("outputTable").end.toScript
      }
    assert(caught.getMessage == "3 join() but 2 on(), the join method must correspond to the on method one by one!")
  }


  test("join-serder") {
    val byzer = Byzer()
    val table1 = byzer.load.format("csv").path("/tmp/jack").options().add("header", "true").end
    val table2 = byzer.load.format("csv").path("/tmp/william").options().add("header", "true").end
    val table3 = byzer.load.format("csv").path("/tmp/admond").options().add("header", "true").end
    table1.namedTableName("table1").end
    table2.namedTableName("table2").end
    table3.namedTableName("table3").end

    val t1 = table1.tableName
    val t2 = table2.tableName
    val t3 = table3.tableName

    val f = byzer.join.
      from(Expr(Some(t1))).
      leftColumns(Expr(Some(s"${t1}.a,${t1}.c"))).
      left(Expr(Some(t2))).
      on_and.add(Expr(Some(s"""${t1}.a=${t2}.b"""))).add(Expr(Some(s"""${t1}.a=${t2}.c"""))).end.
      rightColumns(Expr(Some(s"""${t2}.m"""))).
      right(Expr(Some(t3))).
      on_and.add(Expr(Some(s"""${t1}.a=${t3}.d"""))).add(Expr(Some(s"""${t1}.a=${t3}.e"""))).end.
      rightColumns(Expr(Some(s"""${t3}.n""")))
      .namedTableName("joinTable").end

    val byzerV2 = Byzer().fromJson(f.toJson())

    val script = byzerV2.toScript
    val expectScript = """load csv.`/tmp/jack` where `header`='''true''' as table1;
                         |load csv.`/tmp/william` where `header`='''true''' as table2;
                         |load csv.`/tmp/admond` where `header`='''true''' as table3;
                         |select table1.a,table1.c,table2.m,table3.n
                         |from table1
                         |LEFT OUTER JOIN table2 on (table1.a=table2.b and table1.a=table2.c)
                         |RIGHT OUTER JOIN table3 on (table1.a=table3.d and table1.a=table3.e)
                         |as joinTable;""".stripMargin
    assert(script == expectScript)
    assert(script == byzer.toScript)
    println(script)
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
   * a.last
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

  /**
   * !python conf "schema=st(field(content,string),field(mime,string))";
   * !python env "PYTHON_ENV=source /opt/miniconda3/bin/activate ray1.8.0";
   *
   *
   * run command as Ray.`` where
   * inputTable="day_pv_uv" and
   * outputTable="3ebc4659a63244dd802243a779d9a3e7_0" and
   *
   *
   * code='''#%python
   * #%input=day_pv_uv
   * #%schema=st(field(content,string),field(mime,string))
   * #%env=source /opt/miniconda3/bin/activate ray1.8.0
   *
   * from pyjava.api.mlsql import RayContext,PythonContext
   * ''';
   * select * from 3ebc4659a63244dd802243a779d9a3e7_0 as 3ebc4659a63244dd802243a779d9a3e7;
   */
  test("python") {
    val genCode = Byzer().python.codeWithHint(
      """#%python
        |#%input=day_pv_uv
        |#%schema=st(field(content,string),field(mime,string))
        |#%env=source /opt/miniconda3/bin/activate ray1.8.0
        |
        |from pyjava.api.mlsql import RayContext,PythonContext
        |""".stripMargin).end.toScript
    println(genCode)
  }

  /**
   * !python conf "schema=st(field(content,string),field(mime,string))";
   * !python env "PYTHON_ENV=source /opt/miniconda3/bin/activate ray1.8.0";
   * !python conf "dataMode=model";
   * !python conf "runIn=driver";
   * run command as Ray.`` where
   * inputTable="day_pv_uv" and
   * outputTable="9f5ca0e7d85a4986907213a1c33cbb73_0" and
   *
   *
   * code='''#%python
   * #%input=day_pv_uv
   * #%output=9f5ca0e7d85a4986907213a1c33cbb73
   * #%cache=true
   *
   *
   * #%schema=st(field(content,string),field(mime,string))
   * #%env=source /opt/miniconda3/bin/activate ray1.8.0
   * #%dataMode=model
   * #%runIn=driver
   * from pyjava.api.mlsql import RayContext,PythonContext
   * ''';
   *
   * save overwrite 9f5ca0e7d85a4986907213a1c33cbb73_0 as parquet.`/tmp/__python__/9f5ca0e7d85a4986907213a1c33cbb73`;
   * load parquet.`/tmp/__python__/9f5ca0e7d85a4986907213a1c33cbb73` as 9f5ca0e7d85a4986907213a1c33cbb73;
   */
  test("python2") {
    val genCode = Byzer().python.
      input("day_pv_uv").
      schema("st(field(content,string),field(mime,string))").
      env("source /opt/miniconda3/bin/activate ray1.8.0").
      code("from pyjava.api.mlsql import RayContext,PythonContext").end.toScript
    println(genCode)
  }

  test("tag") {
    val byzer = Byzer()
    val table1 = byzer.load.format("csv").path("/tmp/jack").options().add("header", "true").end.tag("t1")
    val table2 = byzer.load.format("csv").path("/tmp/william").options().add("header", "true").end.tag("t2")
    table1.end
    table2.end

    val t1 = byzer.getByTag("t1")
    println(t1.head.tableName)
  }

  test("toJson") {
    val byzer = Byzer()
    val genCode = byzer.cluster.engine.url("http://127.0.0.1:9003/run/script").owner("jack").end.
      backendStrategy(new JobNumAwareStrategy("")).
      end.
      load.format("csv").path("/tmp/jack").
      options().add("header", "true").end.tag("table1").end.filter.
      and.add(Or(Expr(Some("a>1")), Expr(Some("b>1")))).add(Expr(Some("c==1"))).end.end.
      toJson(true)

    println(genCode)

    var byzer2 = Byzer()
    byzer2 = byzer2.fromJson(genCode)
    assert(byzer2.toJson(true) == genCode)
  }

  test("swap") {
    val byzer = Byzer().
      load.format("csv").path("/tmp/jack").options().add("header", "true").end.tag("load_csv").end.
      filter.or.add(Expr(Some("a=b"))).add(And(Expr(Some("c>2")), Expr(Some("d>10")))).end.tag("filter").end.
      columns.addColumn(Expr(Some("a,c,d"))).tag("select_col").end

    val load_csv = byzer.getByTag("load_csv").head
    val filter = byzer.getByTag("filter").head
    val select_col = byzer.getByTag("select_col").head

    // replace the from table
    select_col.asInstanceOf[Columns].from(load_csv.tableName)
    filter.asInstanceOf[Filter].from(select_col.tableName)

    val genCode = byzer.swapBlock(filter, select_col).toScript

    println(genCode)
  }

  /**
   * load csv.`/tmp/jack` where `header`='''true''' as 58c451473da540e9bf097d8b59fe62d8;
   * select * from 58c451473da540e9bf097d8b59fe62d8 where (a=b or (c>2 and d>10)) as ca0d5f002a5f4d1397658fd20bfa5f1a;
   * select a,c,d from ca0d5f002a5f4d1397658fd20bfa5f1a as 7bd3418bdc4b4a2b9ea64ba6cbe9b03b;
   * select * from 7bd3418bdc4b4a2b9ea64ba6cbe9b03b as newTable;
   * select count(*) as c from newTable as 56d4a29f710a4e3cb6454ed568e29707;
   */
  test("raw") {
    val byzer = Byzer().
      load.format("csv").path("/tmp/jack").options().add("header", "true").end.tag("load_csv").end.
      filter.or.add(Expr(Some("a=b"))).add(And(Expr(Some("c>2")), Expr(Some("d>10")))).end.tag("filter").end.
      columns.addColumn(Expr(Some("a,c,d"))).tag("select_col").end
    val lastTable = byzer.lastTableName
    val genCode = byzer.raw.code(s"select * from ${lastTable} as newTable;").end.
      columns.from("newTable").addColumn(Expr(Some("count(*) as c"))).end.
      toScript

    println(genCode)
  }

  test("test") {
    val s =
      """
        | {
        |        "_clusters": [        {
        |            "_url": "http://127.0.0.1:9003/run/script",
        |            "_params": {},
        |            "extraParams":             {
        |                "sessionPerRequest": "true",
        |                "sessionPerUser": "true",
        |                "includeSchema": "true",
        |                "executeMode": "query",
        |                "fetchType": "take",
        |                "owner": "jack"
        |            }
        |        }],
        |        "refreshTime": 1,
        |        "_backendStrategy":         {
        |            "name": "jobNum",
        |            "tags": ""
        |        }
        |     }""".stripMargin
    println(JSONTool.toJsonStr(JSONTool.parseJson[ClusterMeta](s)))

  }

}
