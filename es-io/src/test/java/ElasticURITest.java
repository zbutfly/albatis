import net.butfly.albatis.elastic.ElasticURI;
import org.junit.Test;

/**
 * Created by hzcominfo67 on 2017/1/19.
 */
public class ElasticURITest {
    @Test
    public void ElasticURI(){
        String uri = "elasticsearch://@10.118.159.45:39300/scattered_data";
        ElasticURI elasticURI =  new ElasticURI(uri);
        System.out.print(elasticURI);
    }
}
