import java.io.IOException;

import org.elasticsearch.action.search.SearchResponse;
import org.junit.Test;

import net.butfly.albatis.elastic.ElasticInput;

/**
 * Created by hzcominfo67 on 2017/1/18.
 */
public class ElasticInputTest {
	@Test
	public void ioInputTest() throws IOException {
		String uri = "elasticsearch://@10.118.159.45:39300/scattered_data";
		try (ElasticInput elasticInput = new ElasticInput(uri);) {
			SearchResponse a = elasticInput.dequeue(1).findFirst().orElse(null);
			System.out.print(a);
			a = elasticInput.dequeue(1).findFirst().orElse(null);
			System.out.print(a);
		}
	}
}
