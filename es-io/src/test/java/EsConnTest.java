import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

import net.butfly.albacore.serder.JsonSerder;
import net.butfly.albatis.elastic.ElasticConnection;

public class EsConnTest {
	private static final String MAPPING_FILE = "C:\\Workspaces\\dataggr\\dataggr\\pumps\\subject\\src\\test\\scripts\\es-mapping.json";

	public static void main(String[] args) throws IOException {
		StringBuilder sb = new StringBuilder();
		String l;
		try (FileReader fr = new FileReader(MAPPING_FILE); BufferedReader r = new BufferedReader(fr);) {
			while ((l = r.readLine()) != null)
				sb.append(l);
		}
		Map<String, Object> mapping = JsonSerder.JSON_MAPPER.der(sb);
		try (ElasticConnection conn = new ElasticConnection("es://hzwacluster@hzwa130:39200/person_test/person");) {
			conn.mapping(mapping, "person");
		}
	}

}
