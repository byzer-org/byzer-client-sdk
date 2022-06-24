## java-client

This is a simple java wrapper for [scala-sdk](https://github.com/allwefantasy/byzer-client-sdk/blob/master/scala-client/README.md) 

## Maven

```xml
<dependency>
    <groupId>tech.mlsql.byzer-client-sdk</groupId>
    <artifactId>java-client</artifactId>
    <version>0.0.3</version>
</dependency>
```

## Example 

```java
String genCode = Byzer.builder().
                load().format("csv").path("/tmp/jack").options().add("header", "true").end().end().
                filter().or().add(Expr.build("a=b")).add(And.build(Expr.build("c>2"), Expr.build("d>10"))).end().end().
                columns().addColumn(Expr.build("split('a',',')[0] as a2")).end().toScript();
System.out.println(genCode);
```

