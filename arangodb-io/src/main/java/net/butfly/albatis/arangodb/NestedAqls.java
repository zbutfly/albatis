package net.butfly.albatis.arangodb;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class NestedAqls {
	public final String aql;
	public final Map<String, Object> params;
	public final Function<Map<String, Object>, List<NestedAqls>> andThens;

	public NestedAqls(String aql, Function<Map<String, Object>, List<NestedAqls>> s) {
		this(aql, null, s);
	}

	public NestedAqls(String aql, Map<String, Object> params, Function<Map<String, Object>, List<NestedAqls>> s) {
		super();
		this.aql = aql;
		this.params = params;
		this.andThens = s;
	}
}
