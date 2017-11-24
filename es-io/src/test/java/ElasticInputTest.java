import java.io.IOException;

import org.elasticsearch.action.search.SearchResponse;
import org.junit.Test;

import net.butfly.albacore.paral.steam.Steam;
import net.butfly.albatis.elastic.ElasticConnection;
import net.butfly.albatis.elastic.ElasticInput;

/**
 * Created by hzcominfo67 on 2017/1/18.
 */
public class ElasticInputTest {
	@Test
	public void ioInputTest() throws IOException {
		String uri = "elasticsearch://@10.118.159.45:39300/scattered_data";
		try (ElasticConnection c = new ElasticConnection(uri); ElasticInput elasticInput = new ElasticInput("", c);) {
			elasticInput.dequeue(ElasticInputTest::t);
			elasticInput.dequeue(ElasticInputTest::t);
			elasticInput.dequeue(ElasticInputTest::t);
			elasticInput.dequeue(ElasticInputTest::t);
		}
	}

	private static long t(Steam<SearchResponse> a) {
		System.out.print(a);
		return 1;
	}
}
