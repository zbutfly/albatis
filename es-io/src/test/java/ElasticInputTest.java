import java.io.IOException;

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
			elasticInput.dequeue(a -> System.out.print(a), 1);
			elasticInput.dequeue(a -> System.out.print(a), 1);
			elasticInput.dequeue(a -> System.out.print(a), 1);
			elasticInput.dequeue(a -> System.out.print(a), 1);
		}
	}
}
