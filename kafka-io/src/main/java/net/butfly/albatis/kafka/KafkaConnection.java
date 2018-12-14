package net.butfly.albatis.kafka;

import com.hzcominfo.albatis.nosql.Connection;
import com.hzcominfo.albatis.nosql.NoSqlConnection;
import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.utils.ZkUtils;
import net.butfly.albacore.exception.ConfigException;
import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albatis.ddl.FieldDesc;
import net.butfly.albatis.ddl.TableDesc;
import org.apache.kafka.common.security.JaasUtils;
import scala.collection.JavaConversions;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

public class KafkaConnection extends NoSqlConnection<Connection> {
    public KafkaConnection(URISpec uri) throws IOException {
        super(uri, "kafka");
    }

    @Override
    public KafkaInput input(TableDesc... topic) throws IOException {
        List<String> l = Colls.list(t -> t.name, topic);
        try {
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

    @Override
    public void construct(String dbName, String table, TableDesc tableDesc, List<FieldDesc> fields) {
        String kafkaUrl = uri.getHost()+"/kafka";
        ZkUtils zkUtils = ZkUtils.apply(kafkaUrl, 30000, 30000, JaasUtils.isZkSecurityEnabled());
        Integer partition = (Integer) tableDesc.construct.get("partition");
        Integer replication = (Integer) tableDesc.construct.get("replication");
        AdminUtils.createTopic(zkUtils, table, partition, replication, new Properties(), new RackAwareMode.Enforced$());
        logger().info("create kafka topic successful");
        zkUtils.close();
    }

    @Override
    public boolean judge(String dbName, String table) {
        String kafkaUrl = uri.getHost()+"/kafka";
        ZkUtils zkUtils = ZkUtils.apply(kafkaUrl, 30000, 30000, JaasUtils.isZkSecurityEnabled());
        List<String> topics = JavaConversions.seqAsJavaList(zkUtils.getAllTopics());
        return topics.contains(table);
    }
}
