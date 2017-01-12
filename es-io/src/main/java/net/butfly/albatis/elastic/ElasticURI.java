package net.butfly.albatis.elastic;

import com.hzcominfo.albatis.search.exception.SearchAPIError;

import java.net.URI;
import java.util.Arrays;
import java.util.Properties;

/**
 * ElasticURI :
 * <p>
 * elasticsearch://clustername@host1:port1,host2:port2/index/type?Properties
 * elasticsearch://clustername@host1:port1,host2:port2?Properties
 * elasticsearch://clustername@host1:port1,host2:port2
 * <p>
 * Created by ljx on 2016/11/25.
 *
 * @author ljx
 * @date 2016/11/25
 */
public class ElasticURI {
    private URI uri;

    public ElasticURI(URI uri) {
        this.uri = uri;
    }

    public String getScheme() {
        return uri.getScheme();
    }

    public String getClusterName() {
        String authority = uri.getAuthority();
        int c = authority.indexOf("@");
        return authority.substring(0, c);
    }

    public String getHost() {
        String authority = uri.getAuthority();
        int c = authority.indexOf("@");
        return authority.substring(c + 1);
    }

    public String getIndex() {
        String[] path = uri.getPath().split("//");
        if (path.length > 1) return path[0];
        else return null;
    }

    public String getType() {
        String[] path = uri.getPath().split("//");
        if (path.length > 2) return path[1];
        else return null;
    }

    public Properties getProperties() {
        String propertiseString = uri.getQuery();
        if (propertiseString == null || propertiseString.isEmpty()) return null;

        String[] propertisies = propertiseString.split("&");
        Properties properties = new Properties();
        Arrays.asList(propertisies).stream().forEach(value -> {
            String[] keyValue = value.split("=");
            if (keyValue.length != 2) throw new SearchAPIError("parameter error " + Arrays.toString(keyValue));
            properties.put(keyValue[0], keyValue[1]);
        });
        return properties;
    }
}
