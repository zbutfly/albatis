package net.butfly.albatis.arangodb;

import java.util.Map;

import net.butfly.albatis.io.Message;

public class AqlMessage extends Message {
	private static final long serialVersionUID = -1862247304022361533L;

	public final String aql;

	public AqlMessage(String aql, Map<String, Object> params) {
		super(params);
		this.aql = aql;
	}
}
