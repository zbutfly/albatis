package net.butfly.albatis.arangodb;

import java.util.Map;

import net.butfly.albatis.io.Message;

public class EdgeMessage extends Message {
	private static final long serialVersionUID = -7016058550224590870L;
	public final Message[] vertexes;

	public EdgeMessage(String tbl, Object key, Map<String, Object> values) {
		super(tbl, key, values);
		vertexes = new Message[] { null, null };
	}

	public void start(String tbl, Object key, Map<String, Object> values) {
		vertexes[0] = new Message(tbl, key, values);
	}

	public void end(String tbl, Object key, Map<String, Object> values) {
		vertexes[1] = new Message(tbl, key, values);
	}
}
