package net.butfly.albatis;

import java.io.IOException;
import java.io.Serializable;

import net.butfly.albacore.io.URISpec;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.Input;
import net.butfly.albatis.io.Output;

public interface Environment extends Connection, Serializable {
	public static final String ENV_URI_PROP = "albatis.io.env.uri";

	<V, O extends Output<V>> O output(URISpec targetUri, TableDesc... tables);

	<V, I extends Input<V>> I input(URISpec targetUri, TableDesc... tables);

	default Connection envConn(URISpec uri) throws IOException {
		return new EnvironmentConnection(uri);
	}

	static Connection connect(URISpec uri) throws IOException {
		return $env$.env.envConn(uri);
	}

	@SuppressWarnings("unchecked")
	static <C extends Environment> C env() {
		return (C) $env$.env;
	}

	class $env$ {
		private final static Environment env = $env$.init();

		private static Environment init() {
			String envUri = System.getProperty("albatis.io.env.uri");
			try {
				return null == envUri ? new EnvironmentStandand() : DriverManager.connect(new URISpec(envUri));
			} catch (IOException e) {
				throw new IllegalArgumentException(e);
			}
		}
	}
}
