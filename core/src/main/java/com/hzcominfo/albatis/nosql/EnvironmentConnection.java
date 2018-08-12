package com.hzcominfo.albatis.nosql;

import java.io.IOException;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.Configs;
import net.butfly.albatis.io.Input;
import net.butfly.albatis.io.Output;
import net.butfly.albatis.io.Rmap;

public interface EnvironmentConnection extends Connection {
	<V, O extends Output<V>> O output(URISpec targetUri, String... table);

	<V, I extends Input<V>> I input(URISpec targetUri, String... table);

	class $env$ {
		@SuppressWarnings("deprecation")
		private static String envUri = Configs.gets("albatis.io.env.uri");
		private static EnvironmentConnection env = null;

		static EnvironmentConnection env() throws IOException {
			if (null == envUri || null != env) return env;
			return env = DriverManager.connect(new URISpec(envUri));
		};

		static Connection connect(URISpec targetSpec) {
			return new ConnectionWrapper(targetSpec);
		}

		private static class ConnectionWrapper implements Connection {
			private final URISpec targetSpec;
			private Connection target = null;

			public ConnectionWrapper(URISpec targetSpec) {
				this.targetSpec = targetSpec;
			}

			@Override
			public void close() throws Exception {
				if (null != target) target.close();

			}

			@Override
			public String defaultSchema() {
				return env.defaultSchema();
			}

			@Override
			public URISpec uri() {
				return targetSpec;
			}

			@Override
			public <M extends Rmap> Input<M> input(String... table) throws IOException {
				Input<M> i = env.input(targetSpec, table);
				if (null != i) return i;
				if (null == target) target = DriverManager.connect(targetSpec);
				return target.input(table);
			}

			@Override
			public <M extends Rmap> Output<M> output() throws IOException {
				Output<M> o = env.output(targetSpec);
				if (null != o) return o;
				if (null == target) target = DriverManager.connect(targetSpec);
				return target.output();
			}
		}
	}
}
