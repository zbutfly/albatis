package net.butfly.albatis.kafka;

import java.io.IOException;
import java.util.List;

import org.apache.kafka.common.security.JaasUtils;

import com.hzcominfo.albatis.nosql.Connection;
import com.hzcominfo.albatis.nosql.DataConnection;

import kafka.utils.ZkUtils;
import net.butfly.albacore.exception.ConfigException;
import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.Input;
import net.butfly.albatis.io.Rmap;
import scala.collection.JavaConversions;

public class KafkaConnection extends DataConnection<Connection> {
    public KafkaConnection(URISpec uri) throws IOException {
        super(uri, "kafka");
    }

    @Override
    public Input<Rmap> createInput(TableDesc... topic) throws IOException {
        List<String> l = Colls.list(t -> t.name, topic);
        try {
        	if (null != uri.getParameter("inputSeriar") && "string".equals(uri.getParameter("inputSeriar")))
        		return new KafkaStrInput("KafkaStrInput", uri, l.toArray(new String[l.size()]));
            return new KafkaInput("KafkaInput", uri, l.toArray(new String[l.size()]));
        } catch (ConfigException e) {
            throw new IOException(e);
        }
    }

    @Override
    public KafkaOutput output(TableDesc... table) throws IOException {
        try {
            return new KafkaOutput("KafkaInput", uri);
        } catch (ConfigException e) {
            throw new IOException(e);
        }
    }

    public static class Driver implements com.hzcominfo.albatis.nosql.Connection.Driver<KafkaConnection> {
        static {
            DriverManager.register(new Driver());
        }

        @Override
        public KafkaConnection connect(URISpec uriSpec) throws IOException {
            return new KafkaConnection(uriSpec);
        }

        @Override
        public List<String> schemas() {
            return Colls.list("kafka");
        }
    }

    @Override
    protected Connection initialize(URISpec uri) {
        return null;
    }

//    @Override
//    public void construct(String dbName, String table, TableDesc tableDesc, List<FieldDesc> fields) {
//        String kafkaUrl = uri.getHost()+"/kafka";
//        ZkUtils zkUtils = ZkUtils.apply(kafkaUrl, 30000, 30000, JaasUtils.isZkSecurityEnabled());
//        Integer partition = (Integer) tableDesc.construct.get("partition");
//        Integer replication = (Integer) tableDesc.construct.get("replication");
//        AdminUtils.createTopic(zkUtils, table, partition, replication, new Properties(), new RackAwareMode.Enforced$());
//        logger().info("create kafka topic successful");
//        zkUtils.close();
//    }

    @Override
    public boolean judge(String dbName, String table) {
        String kafkaUrl = uri.getHost()+"/kafka";
        ZkUtils zkUtils = ZkUtils.apply(kafkaUrl, 30000, 30000, JaasUtils.isZkSecurityEnabled());
        List<String> topics = JavaConversions.seqAsJavaList(zkUtils.getAllTopics());
        return topics.contains(table);
    }
}
