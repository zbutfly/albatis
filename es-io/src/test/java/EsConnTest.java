import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

import net.butfly.albacore.serder.JsonSerder;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.elastic.ElasticConnection;

public class EsConnTest {
	public static final Logger logger = Logger.getLogger(EsConnTest.class);
	private static final String MAPPING_FILE = "C:\\Workspaces\\dataggr\\dataggr\\pumps\\subject\\src\\test\\scripts\\es-mapping.json";

	public static void main(String[] args) throws IOException {
		try (ElasticConnection conn = new ElasticConnection("es://cominfo@hzga152/person_test/person");) {
			conn.mapping(mapping());
		}
	}

	public static Map<String, Object> mapping() {
		StringBuilder sb = new StringBuilder();
		String l;
		try (FileReader fr = new FileReader(MAPPING_FILE); BufferedReader r = new BufferedReader(fr);) {
			while ((l = r.readLine()) != null)
				sb.append(l);
		} catch (FileNotFoundException e) {
			return null;
		} catch (IOException e) {
			return null;
		}
		return JsonSerder.JSON_MAPPER.der(sb);

	}
}
