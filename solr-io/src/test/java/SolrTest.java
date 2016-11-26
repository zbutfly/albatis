import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;

import com.google.common.base.Joiner;

import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.solr.Solrs;
import scala.Tuple3;

public class SolrTest {
	private static final Logger logger = Logger.getLogger(SolrTest.class);

	public static void main(String[] args) throws IOException, URISyntaxException {
		// test("http://10.118.159.106:10301/solr", "personData");
		// test("http://10.118.159.106:10301/solr/personData");
		test("zookeeper://hzga136:9481,hzga137:9481,hzga138:9481", "GAZHK_HZKK_201611");
		// test("zookeeper://hzga136:9481,hzga137:9481,hzga138:9481/GAZHK_HZKK_201611");
	}

	private static void test(String solrURL, String... core) throws URISyntaxException {
		try (SolrClient solr = Solrs.open(solrURL);) {
			QueryResponse resp = core.length == 0 ? solr.query(new SolrQuery("*:*")) : solr.query(core[0], new SolrQuery("*:*"));
			assert (resp.getResults().size() > 0);

			Tuple3<String, String, String[]> t = Solrs.parseSolrURL(solrURL);
			System.err.println("Original URL: \t" + solrURL);
			System.err.println("Base URL: \t" + t._1());
			System.err.println("Default Core: \t" + t._2());
			System.err.println("Cores: " + Joiner.on(',').join(t._3()));
		} catch (SolrServerException | IOException e) {
			logger.error("", e);
		}
	}
}
