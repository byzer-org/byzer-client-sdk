# Scala-client

Scala-client provides a scala-way to build and execute Byzer-lang code.

## Maven

```xml
<dependency>
    <groupId>tech.mlsql.byzer-client-sdk</groupId>
    <artifactId>scala-client_2.12</artifactId>
    <version>0.0.1</version>
</dependency>
```

## Tutorial

### namedTableName

You can specify every output table name otherwize the system will autogenerate a name.

```scala
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
```

## Tag

Tag help you get every block(load/save/select...) and get some information about it.

```scala
test("tag") {
    val byzer = Byzer()
    val table1 = byzer.load.format("csv").path("/tmp/jack").options().add("header", "true").end.tag("t1")
    val table2 = byzer.load.format("csv").path("/tmp/william").options().add("header", "true").end.tag("t2")
    table1.end
    table2.end

    val t1 = byzer.getByTag("t1")    
    println(t1.head.tableName)
  }
```

## Create and Execute Byzer Script

You can setup Byzer engine url and then build byzer script to run.

```scala
test("engine") {
    val byzer = Byzer().cluster().
      engine.url("http://127.0.0.1:9004/run/script").owner("admin").end.
      backendStrategy(new ResourceAwareStrategy("")).
      end
    val script = byzer.variable.name(Expr(Some("data"))).value(Expr(Some(
      """
        |{ "x": 100, "y": 200, "z": 200 ,"dataType":"A group"}
        |{ "x": 120, "y": 100, "z": 260 ,"dataType":"B group"}
        |""".stripMargin))).end.load.format("jsonStr").path("data").namedTableName("table1").end
    val res = script.run()
    println(res.head.returnContent().asString())
  }
```

Configure two engine urls and pick the right engine with resource aware strategy

```scala
 val byzer = Byzer().cluster().
      engine.url("http://127.0.0.1:9004/run/script").owner("admin").end.
      engine.url("http://127.0.0.1:9003/run/script").owner("admin").end.
      backendStrategy(new ResourceAwareStrategy("")).
      end
```

## Scala 

Some examples:

```scala
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

/**
  !python conf "schema=st(field(content,string),field(mime,string))";
 !python env "PYTHON_ENV=source /opt/miniconda3/bin/activate ray1.8.0";


run command as Ray.`` where
inputTable="day_pv_uv" and
outputTable="3ebc4659a63244dd802243a779d9a3e7_0" and


code='''#%python
#%input=day_pv_uv
#%schema=st(field(content,string),field(mime,string))
#%env=source /opt/miniconda3/bin/activate ray1.8.0

from pyjava.api.mlsql import RayContext,PythonContext
''';
select * from 3ebc4659a63244dd802243a779d9a3e7_0 as 3ebc4659a63244dd802243a779d9a3e7;
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
 !python env "PYTHON_ENV=source /opt/miniconda3/bin/activate ray1.8.0";
 !python conf "dataMode=model";
 !python conf "runIn=driver";
run command as Ray.`` where
inputTable="day_pv_uv" and
outputTable="9f5ca0e7d85a4986907213a1c33cbb73_0" and


code='''#%python
#%input=day_pv_uv
#%output=9f5ca0e7d85a4986907213a1c33cbb73
#%cache=true


#%schema=st(field(content,string),field(mime,string))
#%env=source /opt/miniconda3/bin/activate ray1.8.0
#%dataMode=model
#%runIn=driver
from pyjava.api.mlsql import RayContext,PythonContext
''';

save overwrite 9f5ca0e7d85a4986907213a1c33cbb73_0 as parquet.`/tmp/__python__/9f5ca0e7d85a4986907213a1c33cbb73`;
load parquet.`/tmp/__python__/9f5ca0e7d85a4986907213a1c33cbb73` as 9f5ca0e7d85a4986907213a1c33cbb73;
   */
  test("python2") {
    val genCode = Byzer().python.
      input("day_pv_uv").
      schema("st(field(content,string),field(mime,string))").
      env("source /opt/miniconda3/bin/activate ray1.8.0").
      code("from pyjava.api.mlsql import RayContext,PythonContext").end.toScript
    println(genCode)
  }
```