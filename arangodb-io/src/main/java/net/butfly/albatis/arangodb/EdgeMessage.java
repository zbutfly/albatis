package net.butfly.albatis.arangodb;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albatis.io.Message;

public class EdgeMessage extends Message {
	private static final long serialVersionUID = -7016058550224590870L;
	final Message[] vertexes;
	final List<Message> edges;
	Runnable then = null;

	public EdgeMessage(String tbl, Object key, Map<String, Object> values) {
		super(tbl, key, values);
		vertexes = new Message[] { null, null };
		edges = Colls.list();
	}

	public void start(String tbl, Object key, Map<String, Object> values) {
		vertexes[0] = new Message(tbl, key, values);
	}

	public void end(String tbl, Object key, Map<String, Object> values) {
		vertexes[1] = new Message(tbl, key, values);
	}

	public Message start() {
		return vertexes[0];
	}

	public Message end() {
		return vertexes[1];
	}

	public void edge(Message edge) {
		edges.add(edge);
	}

	@Override
	public String toString() {
		return super.toString() //
				+ (null == vertexes[0] ? "" : "\n\tStart Vertex: " + vertexes[0].toString()) //
				+ (null == vertexes[1] ? "" : "\n\t  End Vertex: " + vertexes[1].toString()) //
				+ (edges.isEmpty() ? "" : "\n\t\t And [" + edges.size() + "] other edges.");
	}

	public void edges(Collection<? extends Message> edges) {
		this.edges.addAll(edges);
	}

	public void then(Runnable then) {
		this.then = then;
	}
}
