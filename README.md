# Byzer-client-sdk

Byzer-client-sdk is a Byzer-lang builder which help people to create and execute byzer code.

## Example

```scala
val genCode = Byzer().load.format("csv").path("/tmp/jack").
      options().add("header", "true").end.
      end.toScript
    println(genCode)
```

the output is :

```sql
load csv.`/tmp/jack` where `header`='''true''' as 004f7b1361904755a223a543c613a387;
```

## Tutorial

1 [scala-client](./scala-client/README.md)

