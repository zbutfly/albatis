package net.butfly.albatis;

import java.io.IOException;
import java.io.Serializable;

import net.butfly.albacore.io.URISpec;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.Input;
import net.butfly.albatis.io.Output;
import net.butfly.albatis.io.Rmap;

public interface EnvironmentConnection extends Connection, Serializable {
	<V, O extends Output<V>> O output(URISpec targetUri, TableDesc... tables);

	<V, I extends Input<V>> I input(URISpec targetUri, TableDesc... tables);

	class $env$ {
		private static EnvironmentConnection env = null;

		static EnvironmentConnection env() throws IOException {
			String envUri = System.getProperty("albatis.io.env.uri");
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
			public void close() throws IOException {
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

			@SuppressWarnings("unchecked")
			@Override
			public <M extends Rmap, O extends Output<M>> O outputRaw(TableDesc... table) throws IOException {
				O o = env.output(targetSpec, table);
				if (null != o) return o;
				if (null == target) target = DriverManager.connect(targetSpec);
				return (O) target.output(table);
			}

			@SuppressWarnings("unchecked")
			@Override
			public <M extends Rmap, I extends Input<M>> I inputRaw(TableDesc... table) throws IOException {
				I i = env.input(targetSpec, table);
				if (null != i) return i;
				if (null == target) target = DriverManager.connect(targetSpec);
				return (I) target.input(table);
			}
		}
	}
}
