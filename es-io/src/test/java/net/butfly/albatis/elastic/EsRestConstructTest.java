package net.butfly.albatis.elastic;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albatis.Connection;
import net.butfly.albatis.ddl.DBDesc;
import net.butfly.albatis.ddl.FieldDesc;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.ddl.vals.ValType;
import net.butfly.albatis.io.Output;
import net.butfly.albatis.io.Rmap;

public class EsRestConstructTest {

	public static void main(String[] args) throws IOException {
		EsRestConstructTest test = new EsRestConstructTest();
//		test.buildMappingByRest();
		test.output();
	}

	public void output() {
		String url = "es://es632@172.30.10.31:29300/";
		try (Connection connection = Connection.connect(new URISpec(url));
				Output<Rmap> o = connection.output();) {
			Rmap r = new Rmap("es_rest_test/es_rest_test", "165");
			r.keyField("name");
			r.put("name", "165");
			Sdream<Rmap> items = Sdream.of1(r);
			o.enqueue(items);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void buildMappingByRest() throws IOException {
		String url = "es:rest://es632@172.30.10.31:29200/";
		Map<String, Object> indexConfig = new HashMap<>();
		indexConfig.put("index/type", "es_rest_test_330/es_rest_test_330");
		indexConfig.put("alias", "testRest330");
		indexConfig.put("number_of_shards", 3);
		indexConfig.put("number_of_replicas", 1);
		List<FieldDesc> fields = new ArrayList<>();
		DBDesc dbDesc = DBDesc.of("es_rest_test_330", url);
		TableDesc tableDesc = dbDesc.table("es_rest_test_330");
		ValType type = ValType.of("string");
		FieldDesc f = new FieldDesc(tableDesc, "name", type);
		f.attw("indexed", true);
		fields.add(f);
		FieldDesc[] fieldDescs = new FieldDesc[fields.size()];
		for (int i = 0; i < fields.size(); i++)
			fieldDescs[i] = fields.get(i);
		try (ElasticRestHighLevelConnection connection = new ElasticRestHighLevelConnection(new URISpec(url));) {
			connection.construct(indexConfig, fieldDescs);
		}
	}
}
