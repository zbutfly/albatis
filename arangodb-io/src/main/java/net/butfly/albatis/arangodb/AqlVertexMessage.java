package net.butfly.albatis.arangodb;

import java.text.Format;
import java.text.MessageFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import com.arangodb.entity.BaseDocument;

public class AqlVertexMessage extends AqlNestedMessage {
	private static final long serialVersionUID = -7016058550224590870L;
	protected static final Format AQL_UPSERT = new MessageFormat("upsert '{'_key: @_key} insert {1} update {1} in {0} OPTIONS '{' exclusive: true '}' return NEW"); //

	public AqlVertexMessage(String tbl, Object key, Map<String, Object> vertex) {
		super(tbl, key, vertex);
	}

	@Override
	public String toString() {
		return super.toString();
	}

	private static String parseAqlAsBindParams(Map<String, Object> v) {
		return "{" + v.keySet().stream().map(k -> k + ": @" + k).collect(Collectors.joining(", ")) + "}";
	}

	@Override
	public CompletableFuture<List<BaseDocument>> exec(ArangoConnection conn) {
		String aql = AQL_UPSERT.format(new String[] { table().name, parseAqlAsBindParams(this) });
		return conn.exec(aql, this).thenComposeAsync(l -> {
			nestedResults(l);
			return super.exec(conn);
		});
	}

}
