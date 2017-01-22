package net.butfly.albatis.elastic;

import com.hzcominfo.albatis.nosql.NoSqlConnection;
import net.butfly.albacore.utils.Pair;
import net.butfly.albacore.utils.logger.Logger;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

public class ElasticConnection extends NoSqlConnection<TransportClient> {
    private static final Logger logger = Logger.getLogger(ElasticConnection.class);

    public ElasticConnection(String connection) throws IOException {
        super(new ElasticURI(connection), uri -> {
            Settings.Builder settings = Settings.settingsBuilder();
            settings.put(uri.getParameters());
            settings.put("client.transport.ignore_cluster_name", true);
            TransportClient c = TransportClient.builder().settings(settings).build();
            for (Pair<String, Integer> h : uri.getHosts()) {
                int port = h.value2() == null ? 39300 : h.value2();
                try {
                    c.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(h.value1()), port));
                } catch (UnknownHostException e) {
                    throw new RuntimeException(e);
                }
            }
            return c;
        }, "elasticsearch");
    }

    public ElasticConnection(String url, String... indexAndType) throws IOException {
        this(url);
        if (indexAndType.length > 2) logger.warn("only index and type parsed, other is ignore");
    }

    public String getDefaultIndex() {
        String[] paths = uri.getPathSegs();
        if (paths.length > 0)
            return paths[0];
        else return null;
    }

    public String getDefaultType() {
        String[] paths = uri.getPathSegs();
        if (paths.length > 1)
            return paths[1];
        else return null;
    }

    @Override
    public void close() throws IOException {
        super.close();
        client().close();
    }
}
