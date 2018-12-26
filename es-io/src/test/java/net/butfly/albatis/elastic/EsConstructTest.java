package net.butfly.albatis.elastic;

import net.butfly.albacore.io.URISpec;
import net.butfly.albatis.ddl.DBDesc;
import net.butfly.albatis.ddl.FieldDesc;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.ddl.vals.ValType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EsConstructTest {

    public static void main(String[] args) throws IOException {
        String url ="es://hzcominfo@172.30.10.31:39300/";
        ElasticConnection connection = new ElasticConnection(new URISpec(url));
        Map<String, Object> indexConfig = new HashMap<>();
        indexConfig.put("index/type", "es_test/es_test");
        indexConfig.put("alias","test1");
        indexConfig.put("number_of_shards", 3);
        indexConfig.put("number_of_replicas", 1);
        List<FieldDesc> fields = new ArrayList();
        DBDesc dbDesc = new DBDesc("es_test", url);
        TableDesc tableDesc = new TableDesc(dbDesc, "es_test");
        ValType type = ValType.valueOf("string");
        FieldDesc f = new FieldDesc(tableDesc, "name", type);
        fields.add(f);
        FieldDesc[] fieldDescs = new FieldDesc[fields.size()];
        for (int i = 0; i < fields.size(); i++)
            fieldDescs[i] = fields.get(i);
        connection.construct(indexConfig,fieldDescs);
    }
}