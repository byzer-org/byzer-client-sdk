package tech.mlsql.byzer_client_sdk.j_alng.test;

import org.junit.jupiter.api.Test;
import tech.mlsql.byzer_client_sdk.j_lang.generator.Byzer;
import tech.mlsql.byzer_client_sdk.j_lang.generator.Expr;
import tech.mlsql.byzer_client_sdk.scala_lang.generator.BaseNode;

/**
 * 24/6/2022 WilliamZhu(allwefantasy@gmail.com)
 */
public class JavaTest {
    @Test
    public void testJoin() {
        tech.mlsql.byzer_client_sdk.scala_lang.generator.Byzer builder = Byzer.builder();
        BaseNode tableLoad1 = builder.load().format("csv").path("/tmp/jack").options().add("header", "true").end().tag("table1");
        BaseNode  tableLoad2 = builder.load().format("csv").path("/tmp/admond").options().add("header", "true").end().tag("table2");

        tableLoad1.end();
        tableLoad2.end();

        String table1 = tableLoad1.tableName();
        String table2 = tableLoad2.tableName();

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
