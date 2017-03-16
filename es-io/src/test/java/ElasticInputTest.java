import java.io.IOException;
import java.util.stream.Stream;

import org.elasticsearch.action.search.SearchResponse;
import org.junit.Test;

import net.butfly.albatis.elastic.ElasticInput;

/**
 * Created by hzcominfo67 on 2017/1/18.
 */
@Deprecated
public class ElasticInputTest {
	@Test
	public void ioInputTest() throws IOException {
		String uri = "elasticsearch://@10.118.159.45:39300/scattered_data";
		try (ElasticInput elasticInput = new ElasticInput(uri);) {
			elasticInput.dequeue(ElasticInputTest::t, 1);
			elasticInput.dequeue(ElasticInputTest::t, 1);
			elasticInput.dequeue(ElasticInputTest::t, 1);
			elasticInput.dequeue(ElasticInputTest::t, 1);
		}
	}

	private static long t(Stream<SearchResponse> a) {
		System.out.print(a);
		return 1;
	}
}
