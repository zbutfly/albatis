package com.hzcominfo.dataggr.uniquery.mongo.test;

import com.google.gson.JsonObject;
import com.hzcominfo.dataggr.uniquery.SqlExplainer;
import com.hzcominfo.dataggr.uniquery.mongo.MongoQuery;
import com.hzcominfo.dataggr.uniquery.mongo.MongoQueryVistor;
import com.mongodb.*;
import net.butfly.albacore.io.URISpec;
import net.butfly.albatis.mongodb.MongoConnection;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

public class MongoQueryTest {
    private static final String uri = "mongodb://yjdb:yjdb1234@172.30.10.101:22001/yjdb/YJDB_GAZHK_WBXT_SWRY_XXB?readPreference=primary";
    private MongoConnection connection;
    private DBCollection collection;

    @Before
    public void b() throws IOException {
        connection = new MongoConnection(new URISpec(uri));
        collection = connection.collection();
    }

    @After
    public void a() {
        if (null != connection) connection.close();
    }

    @Test
    public void t1() throws IOException {
        System.out.println(collection.getFullName() + " count: " + collection.count());
        // like
//        String like = "浙A%";
        String like = "智力%";
        if (!like.startsWith("%")) like = "^" + like;
        if (!like.endsWith("%")) like = like + "$";
//        like = like.replaceAll("_", ".").replaceAll("%", ".*");
        like = like.replaceAll("_", ".").replaceAll("%", ".*");
        System.out.println("3 like: " + like);
        Pattern pattern = Pattern.compile(like, Pattern.CASE_INSENSITIVE);
        QueryBuilder builder = QueryBuilder.start("SERVICE_NAME").regex(pattern);
        DBObject query = builder.get();
        System.out.println("query:" + query);
        DBObject fields = new BasicDBObject();

        fields.put("SERVICE_CODE", 1);
        fields.put("USER_NAME", 1);
        DBCursor cursor = collection.find(query, fields);
//        cursor.skip(10);
//        cursor.limit(20);

        AtomicInteger idx = new AtomicInteger(0);
        cursor.forEach(o -> System.out.println(idx.incrementAndGet() + " --->  " + o));
        connection.close();
//        System.out.println(builder.get());
    }

    /* fields test */
    @Test
    public void f1() {
        /*unrecognized*/
//        String sql = "select * from YJDB_GAZHK_WBXT_SWRY_XXB";
        /*recognized*/
        String sql = "select _id, CERTIFICATE_CODE, USER_NAME from YJDB_GAZHK_WBXT_SWRY_XXB";
        /*impure*/
//        String sql = "select *, _id, CERTIFICATE_CODE, USER_NAME from YJDB_GAZHK_WBXT_SWRY_XXB";

        JsonObject json = SqlExplainer.explain(sql);
        MongoQuery mq = new MongoQueryVistor(new MongoQuery(), json).get();
        DBObject query = mq.getQuery();
        DBObject fields = mq.getFields();
        DBCursor cursor = collection.find(query, fields);
        AtomicInteger idx = new AtomicInteger();
        cursor.iterator().forEachRemaining(o -> {
            System.out.println(idx.incrementAndGet() + "--->" + o);
        });
    }


}
