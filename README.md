# Byzer-client-sdk

Byzer-client-sdk is a Byzer-lang builder which help people to create and execute byzer code.

## Example

## Generate Byzer-lang code

```scala
val genCode = Byzer().load.format("csv").path("/tmp/jack").
      options().add("header", "true").end.
      end.toScript
println(genCode)
```

the output is :

```sql
load csv.`/tmp/jack` where `header`='''true''' 
as 004f7b1361904755a223a543c613a387;
```

## Execute Byzer-lang code

```scala
val byzer = Byzer().cluster().
      engine.url("http://127.0.0.1:9004/run/script").owner("admin").end.
      engine.url("http://127.0.0.1:9003/run/script").owner("admin").end.
      backendStrategy(new ResourceAwareStrategy("")).
      end
val script = byzer.variable.name(Expr(Some("data"))).value(Expr(Some(
    """
    |{ "x": 100, "y": 200, "z": 200 ,"dataType":"A group"}
    |{ "x": 120, "y": 100, "z": 260 ,"dataType":"B group"}
    |""".stripMargin))).end.load.format("jsonStr").path("data").namedTableName("table1").end
val res = script.run()
println(res.head.returnContent().asString())
```


## Tutorial

1. [scala-client](./scala-client/README.md)
2. java-client
3. go-client
4. rust-client
5. typescript-client


