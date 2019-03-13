package net.butfly.albatis;

import java.io.IOException;

import net.butfly.albacore.io.URISpec;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.Input;
import net.butfly.albatis.io.Output;

class EnvironmentStandand implements Environment {
	private static final long serialVersionUID = -6812982665370855324L;

	// @Override
	// public Object submit(Callable<Object> task) throws Exception {
	// return task.call();
	// }

	@Override
	public String defaultSchema() {
		return null;
	}

	@Override
	public URISpec uri() {
		return null;
	}

	@Override
	public void close() throws IOException {}

	@Override
	public <V, O extends Output<V>> O output(URISpec targetUri, TableDesc... tables) {
		throw new UnsupportedOperationException();
	}

	@Override
	public <V, I extends Input<V>> I input(URISpec targetUri, TableDesc... tables) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Connection envConn(URISpec uri) throws IOException {
		return DriverManager.connect(uri);
	}
}
