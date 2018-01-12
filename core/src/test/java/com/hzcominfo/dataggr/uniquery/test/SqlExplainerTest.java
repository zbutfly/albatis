package com.hzcominfo.dataggr.uniquery.test;

import com.google.gson.JsonObject;
import com.hzcominfo.dataggr.uniquery.SqlExplainer;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.junit.Test;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SqlExplainerTest {

    @Test
    public void t2() throws Exception {
        String sql = "select a.p.name AS n, b.p.age As a, p.address from a.people p where age > 30  and name like '%姓名%' order by age desc, name asc limit 1000 offset 10";
        JsonObject object = SqlExplainer.explain(sql);
        System.out.println(object);
    }

    @Test
    public void t5() throws Exception {
//        String sql = "SELECT * FROM test_hzwa.RC_WA_BASIC_ES_1002 WHERE COLLECTION_EQUIPMENTID_s.keyword IN ('728489494000FE201231D') LIMIT 1000";
        String sql = "SELECT LG._._._.NAME FROM test_hzwa.RC_WA_BASIC_ES_1002 WHERE COLLECTION_EQUIPMENTID_s._._._.keyword IN ('728489494000FE201231D') LIMIT 1000";
        JsonObject object = SqlExplainer.explain(sql);
        System.out.println(object);
    }

    @Test
    public void p1() throws Exception {
//        String sql = "select * from tbl where name like '%tdl%'";
//        String sql = "select name, age, sex as s from tbl where name like '%tdl%'";
        String sql = "select name, age, *, sex as s from tbl where name like '%tdl%'";
//        String sql = "select * from tbl AS t, wsy as w, abc where name like '%tdl%'";
//        String sql = "select * from tbl1 as x, tbl2 as y, tbl3 where name like '%tdl%'";
        JsonObject object = SqlExplainer.explain(sql);
    }

    @Test
    public void p2() throws Exception {
        String sql = "SELECT Customer, OrderDate, SUM(OrderPrice) FROM Orders GROUP BY Customer,OrderDate";
        JsonObject object = SqlExplainer.explain(sql);
        System.out.println(object);
    }

    @Test
    public void p3() throws Exception {
        String sql = "SELECT Customer,SUM(OrderPrice) FROM Orders GROUP BY Customer HAVING SUM(OrderPrice)<2000 limit 1000 offset 100";
        JsonObject object = SqlExplainer.explain(sql);
        System.out.println(object);
    }

    @Test
    public void p4() throws Exception {
        String sql = "SELECT Company, OrderNumber FROM Orders,tbl2,tbl3 ORDER BY Company desc, xxx desc limit 1000 offset 100";
        JsonObject object = SqlExplainer.explain(sql);
        System.out.println(object);
    }

    @Test
    public void p5() throws Exception {
        String sql = "SELECT DISTINCT Company, name FROM Orders order by xxx limit 1000 offset 100";
        JsonObject object = SqlExplainer.explain(sql);
        System.out.println(object);
    }

    @Test
    public void p6() throws Exception {
        String sql = "SELECT name as n, Company as c, pure FROM Orders order by xxx limit 1000 offset 100";
        JsonObject object = SqlExplainer.explain(sql);
        System.out.println(object);
    }

    /**
     * where
     * @throws Exception
     */
    @Test
    public void w1() throws Exception {
//        String sql = "select * from t where name = 'ci' and age = 30";
//        String sql = "select * from t where (name = 'ci' or age < 30) and (name = 'xx' or age >= 20) and (name like '%yy%' or age <> 10)";
//        String sql = "select * from t where name is not NULL";
//        String sql = "select * from t where name is null";
//        String sql = "select * from t where age >= 10";
//        String sql = "select * from t where name in (a, b, c, d, e)"; //todo wrong syntax
        String sql = "select * from t where name not in ('a', 'b', 'c', 'd', 'e') and (name is NOT NULL or age < 20) or (name = 'xx' or age >= 20) and (name like '%yy%' or age <> 10)";
//        String sql = "select * from t where name in ('a', 'b', 'c', 'd', 'e')";
        JsonObject object = SqlExplainer.explain(sql);
        System.out.println(object);
        JsonObject o = object.getAsJsonObject("where").getAsJsonArray("and").get(0).getAsJsonObject()
                .getAsJsonArray("and").get(0).getAsJsonObject().getAsJsonArray("or")
                .get(0).getAsJsonObject().getAsJsonObject("equals");
        o.entrySet().forEach(entry -> {
            System.out.println(entry.getKey());
            System.out.println(entry.getValue().getAsString());
        });
    }

    /**
     * group by
     * @throws Exception
     */
    @Test
    public void g1() throws Exception {
        String sql = "SELECT Customer,OrderPrice FROM Orders GROUP BY Customer, xxxx";
        JsonObject object = SqlExplainer.explain(sql);
    }

    /**
     * having
     * @throws Exception
     */
    @Test
    public void h1() throws Exception {
//        String sql = "SELECT name_s, sell_i FROM saleroom GROUP BY name_s, year_i HAVING year_i = 2017";
//        String sql = "SELECT name_s, sell_i FROM saleroom GROUP BY name_s, year_i HAVING count(sell_i) > 2000 and name like '%Jam%'";
        String sql = "SELECT name_s, sell_i FROM saleroom GROUP BY name_s, year_i HAVING count(sell_i) is not null and name like '%Jam%'";
        JsonObject object = SqlExplainer.explain(sql);
        System.out.println(object);
    }

    /**
     * order by
     * @throws Exception  order by
     */
    @Test
    public void ob1() throws Exception {
//        String sql = "SELECT Company, OrderNumber FROM Orders ORDER BY Company ";
//        String sql = "SELECT Company, OrderNumber FROM Orders ORDER BY Company DESC";
        String sql = "SELECT Company, OrderNumber FROM Orders ORDER BY Company DESC, ABC DESC";
//        String sql = "SELECT Company, OrderNumber FROM Orders ORDER BY Company DESC, OrderNumber ASC";
//        String sql = "SELECT Company, OrderNumber FROM Orders ORDER BY Company Asc, OrderNumber desc";
        JsonObject object = SqlExplainer.explain(sql);
    }

    /**
     * offset
     * @throws Exception
     */
    @Test
    public void of1() throws Exception {
//        String sql = "SELECT Customer,OrderPrice FROM Orders";
        String sql = "SELECT Customer,OrderPrice FROM Orders limit 99887766";
//        String sql = "SELECT Customer,OrderPrice FROM Orders limit 92233720368547758070 offset 9223372036854775807";
//        String sql = "SELECT Customer,OrderPrice FROM Orders limit 9223372036854775807 offset 1024";
        JsonObject object = SqlExplainer.explain(sql);
    }

    /**
     * limit
     * @throws Exception
     */
    @Test
    public void l1() throws Exception {
//        String sql = "SELECT Customer,OrderPrice FROM Orders limit 92233720368547758070 offset 9223372036854775807";
        String sql = "SELECT Customer,OrderPrice FROM Orders limit 9223372036854775807 offset 1024";
        JsonObject object = SqlExplainer.explain(sql);
    }

    @Test
    public void b1() throws Exception {
//        String sql = "SELECT Customer,OrderPrice FROM Orders limit 92233720368547758070 offset 9223372036854775807";
        String sql = "SELECT * FROM tttt group by a.b";
        JsonObject object = SqlExplainer.explain(sql);
        System.out.println(object);
    }

    @Test
    public void b2() throws Exception {
//        String sql = "SELECT Customer,OrderPrice FROM Orders limit 92233720368547758070 offset 9223372036854775807";
        String sql = "SELECT * FROM tttt where name >= 'haha' ";
        JsonObject object = SqlExplainer.explain(sql);
        System.out.println(object);
    }

    @Test
    public void q1() {
        String sql = "select * from student where ? in (?,?)";
//        String sql = "select name, ?, age from people where ? is not null and age > ? or name = ? or age between ? and ?";
        JsonObject object = SqlExplainer.explain(sql, "name", "小明", "小芳");
        System.out.println(object);

        String str = object.toString();

    }

    @Test
    public void n1() {
        String sql = "select * from student where age > 27";
        CalcitePrepare.Query query = CalcitePrepare.Query.of(sql);
        System.out.println(query);
    }

    @Test
    public void r1() {
        String s = "name: $${1}, age: $${2}, girl: $${3}";
        Object[] params = {"孙露", 27, true};
        Pattern pattern = Pattern.compile("\\$\\$\\{\\d+}");
        Matcher matcher = pattern.matcher(s);
        while (matcher.find()) {
            String group = matcher.group();
            int index = Integer.valueOf(group.substring(0, group.length() - 1).substring(3));
            s = s.replace(group, params[index-1].toString());
        }
        System.out.println(s);
    }

    @Test
    public void u1() {
        String sql = "select keyword(u.d.f) from person where keyword(home.address.first.\"second\") = 'hangzhou'";
//        String sql = "select keyword(u.d.f) from person where keyword(home.address.secon) = 'hangzhou'";
//        String sql = "select keyword(u.d.f) from person where keyword(home.address.secon.thrid) = 'hangzhou'";
//        String sql = "select keyword(u.n.f) from person where keyword(h.a.s.address.secon) = 'hangzhou' order by keyword(o.b.f) desc";
//        String sql = "select keyword(user.name.first) from person where keyword(home.addr.secon) = 'hangzhou'";
//        String sql = "select keyword(a.b.c) from person where keyword(d.e.f) = 'kaka'";
//        String sql = "select keyword(t1.PHONE_NUMBER.aa, t2.PHONE_MAC) from DATA_PHONE t1 inner join DATA_PHONE_MAC t2 on t1.ID = t2.ID";
//        String sql = "select keyword(t1.PHONE_NUMBER.aa) from DATA_PHONE t1 ";
        JsonObject object = SqlExplainer.explain(sql);
        System.out.println(object);
    }

    @Test
    public void u2() {
        /** udf in fields */
//        String sql = "select keyword(u.n.f) from person";
//        String sql = "select keyword(u.n.f) as udf from person";
//        String sql = "select count(u.n.f) from person";
//        String sql = "select count(*) from person";
        /** udf in order by */
//        String sql = "select keyword(u.n.f) from person order by order_field";
//        String sql = "select keyword(u.n.f) from person order by o.b.f";
//        String sql = "select keyword(u.n.f) from person order by o.b.f desc";
//        String sql = "select keyword(u.n.f) from person order by keyword(o.b.f)";
//        String sql = "select keyword(u.n.f) from person order by keyword(o.b.f) desc";

        /** unary condition test */
//        String sql = "select u.n.f from person where h.a.s.address.\"second\" is not null";
//        String sql = "select u.n.f from person where keyword(h.a.s.address.\"second\") is not null";
        /** binary condition test */
//        String sql = "select u.n.f from person where h.a.s.address.\"second\" = 'hangzhou'";
//        String sql = "select u.n.f from person where keyword(h.a.s.address.\"second\") > 'hangzhou'";
        /** ternary condition test */
//        String sql = "select u.n.f from person where h.a.s.address.\"second\" between 1 and 9";
//        String sql = "select u.n.f from person where keyword(h.a.s.address.\"second\") between 1 and 9";
        /** multiple condition test */
//        String sql = "select u.n.f from person where h.a.s.address.\"second\" in (1, 2, 3, 4, 5)";
//        String sql = "select u.n.f from person where keyword(h.a.s.address.\"second\") in (1, 2, 3, 4, 5)";
        /** udf in group by */
        String sql = "select u.n.f from person group by u.d.f";
//        String sql = "select u.n.f from person group by keyword(u.d.f)";
        JsonObject object = SqlExplainer.explain(sql);
        System.out.println(object);
    }

    @Test
    public void a1() {
        String sql = "select a.b as ab, ? as cd, e.f, g.h from t";
        JsonObject object = SqlExplainer.explain(sql, "rzsj");
//        String sql = "select a.b as ab, ? as cd, e.f, g.h as ? from t"; // 不支持这种语法
//        JsonObject object = SqlExplainer.explain(sql, "rzsj", "gh");
        System.out.println(object);
    }

}


// https://github.com/publicclass/sql-where