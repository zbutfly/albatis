import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;

import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.solr.SolrConnection;

public class SolrTest {
	private static final Logger logger = Logger.getLogger(SolrTest.class);

	public static void main(String[] args) throws IOException, URISyntaxException {
		// test("http://10.118.159.106:10301/solr", "personData");
		// test("http://10.118.159.106:10301/solr/personData");
		test("zk://hzga136:9481,hzga137:9481,hzga138:9481", "GAZHK_HZKK_201611");
		// test("zk://hzga136:9481,hzga137:9481,hzga138:9481/GAZHK_HZKK_201611");
	}

	private static void test(String solrURL, String... core) throws URISyntaxException {
		try (SolrConnection solr = new SolrConnection(solrURL);) {
			QueryResponse resp = core.length == 0 ? solr.client.query(new SolrQuery("*:*")) : solr.client.query(core[0], new SolrQuery("*:*"));
			assert (resp.getResults().size() > 0);
			System.err.println("Original URL: \t" + solrURL);
			System.err.println("Base URL: \t" + solr.getBase());
			System.err.println("Default Core: \t" + solr.getDefaultColl());
			System.err.println("Cores: " + String.join(",", solr.getColls()));
		} catch (SolrServerException | IOException e) {
			logger.error("", e);
		}
	}
}
