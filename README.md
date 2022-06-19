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

## Ser/Deser as Json

The user can dumps the Byzer builder as Json Format and restore from the Json string again.
Here is the example:

```scala
val byzer = Byzer()
val jsonString = byzer.cluster.engine.url("http://127.0.0.1:9003/run/script").owner("jack").end.
backendStrategy(new JobNumAwareStrategy("")).
end.
load.format("csv").path("/tmp/jack").
options().add("header", "true").end.tag("table1").end.filter.
and.add(Or(Expr(Some("a>1")), Expr(Some("b>1")))).add(Expr(Some("c==1"))).end.end.
toJson(true)

var byzer2 = Byzer()
byzer2 = byzer2.fromJson(jsonString)
assert(byzer2.toJson(true) == jsonString)
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
        |""".stripMargin))).end.load.format("jsonStr").path("data").namedTableName("table1").tag("load_json").end.
      columns.addColumn(Expr(Some("x"))).end

// run all script
val res = script.run()
println(res.head.returnContent().asString())

// Generate code unitl the statement with tag `load_json` and run it
val res1 = script.runUntilTag("load_json")
println(res1.head.returnContent().asString())

// Only run statement with tag `xxxx`. 
val res2 = script.runWithTag("xxxx")
println(res2.head.returnContent().asString())
```

## commands 

```scala
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

println(byzer.commands.showVersion)

println(byzer.commands.showResource)

println(byzer.commands.showJobs)
```

## Tutorial

1. [scala-client](./scala-client/README.md)
2. [java-client](./java-client/README.md)
3. go-client
4. rust-client
5. typescript-client


