package net.butfly.albatis.kafka;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.hzcominfo.albatis.nosql.Connection;
import com.hzcominfo.albatis.nosql.NoSqlConnection;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.utils.ZkUtils;
import net.butfly.albacore.exception.ConfigException;
import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albatis.ddl.TableCustomSet;
import net.butfly.albatis.ddl.TableDesc;
import org.apache.kafka.common.security.JaasUtils;

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

    /**
     * create kafka topic
     *
     * @param url
     * @param table
     * @param tableCustomSet
     */
    private void createKafkaTopic(String url, String table, TableCustomSet tableCustomSet) {
        if (url.endsWith("kafka")) {
            //url: zookeeper地址：端口号
            String kafkaUrl = url.substring(url.indexOf("//") + 2);
            //sessionTimeoutMs, connectionTimeoutMs
            ZkUtils zkUtils = ZkUtils.apply(kafkaUrl, 30000, 30000, JaasUtils.isZkSecurityEnabled());
            JSONObject topic = JSONObject.parseObject(JSON.toJSONString(tableCustomSet.getOptions().get("topic")), JSONObject.class);
            AdminUtils.createTopic(zkUtils, table, (Integer) topic.get("partition"), (Integer) topic.get("replication"), new Properties(), new RackAwareMode.Enforced$());
            logger().info("create kafka topic successful");
            zkUtils.close();
        }
    }
}
