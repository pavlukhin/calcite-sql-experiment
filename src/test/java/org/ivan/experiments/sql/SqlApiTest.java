package org.ivan.experiments.sql;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class SqlApiTest {
    static {
        System.setProperty("calcite.debug", "true");
    }

    @Test
    public void testSelectAll() throws Exception {
        SqlApi sqlApi = new SqlApi();

        sqlApi.setupTable("Person", ImmutableList.of(
                new SqlTuple(ImmutableMap.of("ID", 1, "NAME", "ivan", "AGE", 30)),
                new SqlTuple(ImmutableMap.of("ID", 2, "NAME", "tanya", "AGE", 30)),
                new SqlTuple(ImmutableMap.of("ID", 3, "NAME", "serega", "AGE", 4))
        ));

//        List<List<Object>> res = sqlApi.query("select id, name, age from Person");
//        System.err.println(res);
//
//        assertEquals(3, res.size());

//        System.err.println(sqlApi.query("select name, age from Person"));
//
//        System.err.println(sqlApi.query("select * from Person"));

        System.err.println(sqlApi.query("select id, name, age + 10 from Person where age < 10"));
    }

    @Test
    public void testSelectWhere() throws Exception {
        SqlApi sqlApi = new SqlApi();

        sqlApi.setupTable("Person", ImmutableList.of(
                new SqlTuple(ImmutableMap.of("ID", 1, "NAME", "ivan", "AGE", 30)),
                new SqlTuple(ImmutableMap.of("ID", 2, "NAME", "tanya", "AGE", 30)),
                new SqlTuple(ImmutableMap.of("ID", 3, "NAME", "serega", "AGE", 4))
        ));

        List<List<Object>> res = sqlApi.query("select * from Person where age = 30");
        System.err.println(res);

        assertEquals(2, res.size());

        System.err.println(sqlApi.query("select * from Person where age = 4"));
        System.err.println(sqlApi.query("select * from Person where age = 1"));
        System.err.println(sqlApi.query("select * from Person where id = 1"));
    }

    @Test
    public void testSelectGrouping() throws Exception {
        SqlApi sqlApi = new SqlApi();

        sqlApi.setupTable("Person", ImmutableList.of(
                new SqlTuple(ImmutableMap.of("ID", 1, "NAME", "ivan", "AGE", 30)),
                new SqlTuple(ImmutableMap.of("ID", 2, "NAME", "tanya", "AGE", 30)),
                new SqlTuple(ImmutableMap.of("ID", 3, "NAME", "serega", "AGE", 4))
        ));

        System.err.println(sqlApi.query("select age, count(1) from Person group by age"));
    }

    @Test
    public void testJoin() throws Exception {
        SqlApi sqlApi = new SqlApi();

        sqlApi.setupTable("Person", ImmutableList.of(
                new SqlTuple(ImmutableMap.of("ID", 1, "NAME", "ivan")),
                new SqlTuple(ImmutableMap.of("ID", 2, "NAME", "tanya"))));

        sqlApi.setupTable("Notes", ImmutableList.of(
                new SqlTuple(ImmutableMap.of("PID", 1, "NOTE", "Bla-bla-bla"))));

        System.err.println(sqlApi.query("select p.name, n.note from Person p join Notes n on p.id = n.pid"));
    }

    @Test
    public void testSubQuery() throws Exception {
        SqlApi sqlApi = new SqlApi();

        sqlApi.setupTable("Person", ImmutableList.of(
                new SqlTuple(ImmutableMap.of("ID", 1, "NAME", "ivan", "AGE", 30)),
                new SqlTuple(ImmutableMap.of("ID", 2, "NAME", "tanya", "AGE", 30))));

        System.err.println(sqlApi.query("select name from Person where age = ((select max(age) from Person) - 10)"));
    }
}