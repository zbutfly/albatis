package net.butfly.albatis.arangodb;

import java.text.Format;
import java.text.MessageFormat;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.arangodb.entity.BaseDocument;

import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albatis.io.Message;

public class EdgeMessage extends Message {
	private static final long serialVersionUID = -7016058550224590870L;

	private static final Format AQL_UPSERT = new MessageFormat("upsert '{'_key: @_key} insert {1} update {1} in {0} return NEW"); //
	private EdgeMessage start, end;
	final String aql;
	private Function<Map<String, Object>, List<EdgeMessage>> nestedThen;

	public EdgeMessage(String tbl, Object key, Map<String, Object> vertex) {
		super(tbl, key, vertex);
		vertex.put("_key", key);
		aql = AQL_UPSERT.format(new String[] { tbl, parseAqlAsBindParams(vertex) });
	}

	public EdgeMessage(String aql, Map<String, Object> params) {
		super(params);
		this.aql = aql;
	}

	public EdgeMessage(String aql) {
		super();
		this.aql = aql;
	}

	public void start(String tbl, Object key, Map<String, Object> values) {
		start = new EdgeMessage(tbl, key, values);
	}

	public void end(String tbl, Object key, Map<String, Object> values) {
		end = new EdgeMessage(tbl, key, values);
	}

	public EdgeMessage start() {
		return start;
	}

	public EdgeMessage end() {
		return end;
	}

	public boolean nested() {
		return null != nestedThen;
	}

	public void then(Function<Map<String, Object>, List<EdgeMessage>> nestedThen) {
		this.nestedThen = nestedThen;
	}

	public List<EdgeMessage> applyThen(BaseDocument doc) {
		return null == nestedThen ? Colls.list() : nestedThen.apply(null == doc ? Maps.of() : doc.getProperties());
	}

	@Override
	public String toString() {
		return super.toString() //
				+ (null == start ? "" : "\n\tStart Vertex: " + start.toString()) //
				+ (null == end ? "" : "\n\t  End Vertex: " + end.toString());
	}

	private static String parseAqlAsBindParams(Map<String, Object> v) {
		return "{" + v.keySet().stream().map(

				k -> k + ": @" + k).collect(Collectors.joining(", ")) + "}";
	}
}
