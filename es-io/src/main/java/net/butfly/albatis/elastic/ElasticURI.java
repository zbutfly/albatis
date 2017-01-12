package net.butfly.albatis.elastic;

import java.net.URI;
import java.util.Properties;

import org.elasticsearch.common.settings.Settings;

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

	Settings getSettings(Properties properties) {
		Settings.Builder settings = Settings.settingsBuilder();
		if (properties != null) settings.put(properties);
		settings.put("client.transport.ignore_cluster_name", true);
		return settings.build();
	}
}
