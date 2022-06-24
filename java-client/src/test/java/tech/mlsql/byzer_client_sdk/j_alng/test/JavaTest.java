package tech.mlsql.byzer_client_sdk.j_alng.test;

import org.junit.jupiter.api.Test;
import tech.mlsql.byzer_client_sdk.j_lang.generator.Byzer;
import tech.mlsql.byzer_client_sdk.j_lang.generator.Expr;

/**
 * 24/6/2022 WilliamZhu(allwefantasy@gmail.com)
 */
public class JavaTest {
    @Test
    public void testJoin() {
        tech.mlsql.byzer_client_sdk.scala_lang.generator.Byzer builder = Byzer.builder();
        builder.load().format("csv").path("/tmp/jack").options().add("header", "true").end().tag("table1").end();
        builder.load().format("csv").path("/tmp/admond").options().add("header", "true").end().tag("table2").end();

        String table1 = builder.getByTag("table1").head().tableName();
        String table2 = builder.getByTag("table2").head().tableName();

        System.out.println(table1);
        System.out.println(table2);

        String genCode = builder.join().
                from(Expr.build(table1)).
                left(Expr.build(table2)).
                on(Expr.build(String.format("%s.a=%s.b", table1, table2))).
                leftColumns(Expr.build(String.format("%s.a,%s.c", table1, table2))).
                rightColumns(Expr.build(String.format("%s.m", table2))).end()
                .toScript();
        System.out.println(genCode);
    }
}
