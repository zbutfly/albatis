package net.butfly.albatis.arangodb;

import java.util.Map;

import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albatis.io.Message;

public class EdgeMessage extends Message {
	private static final long serialVersionUID = -7016058550224590870L;
	public final Map<String, Object> startVertex;
	public final Map<String, Object> endVertex;

	public EdgeMessage(Map<String, Object> startVertex, Map<String, Object> endVertex) {
		super();
		this.startVertex = Maps.of();
		this.endVertex = Maps.of();
	}
}
