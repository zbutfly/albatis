package net.butfly.albatis.elastic;

import net.butfly.albacore.io.URISpec;
import net.butfly.albatis.ddl.FieldDesc;
import net.butfly.albatis.ddl.Qualifier;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.ddl.vals.ValType;

import java.io.IOException;

public class Elastic7Test {
    public static void main(String[] args) throws IOException {
//        ElasticConnection connection = new ElasticConnection(new URISpec("es://idatafusionlite@10.19.120.67:39300/"));
        ElasticRestHighLevelConnection connection = new ElasticRestHighLevelConnection(new URISpec("es:rest://idatafusionlite@10.19.120.67:39200/"));
        FieldDesc[] descs = new FieldDesc[3];
        FieldDesc _from = new FieldDesc(TableDesc.dummy("es77_rest_test"), "from_table", ValType.of("string"), false, false, false);
        _from.attw("indexed", true);
        FieldDesc _from_format = new FieldDesc(TableDesc.dummy("es77_rest_test"), "from_table_format", ValType.of("string"), false, false, false);
        _from_format.attw("indexed", true);
        FieldDesc dateField = new FieldDesc(TableDesc.dummy("es77_rest_test"), "update_time", ValType.of("date"), false, false, false);
        dateField.attw("indexed", false);
        descs[0] = _from;
        descs[1] = _from_format;
        descs[2] = dateField;
        connection.construct(new Qualifier("es77_rest_test"),descs);
    }
}
