package net.butfly.albatis.arangodb;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.arangodb.ArangoCursorAsync;
import com.arangodb.ArangoDBException;
import com.arangodb.entity.BaseDocument;

import net.butfly.albacore.io.lambda.Function;
import net.butfly.albacore.paral.Exeter;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.io.OddInput;
import net.butfly.albatis.io.Rmap;

public class ArangoInput extends net.butfly.albacore.base.Namedly implements OddInput<Rmap> {
	private static final long serialVersionUID = 2242853649760090074L;
	private static final Logger logger = Logger.getLogger(ArangoInput.class);
	private ArangoConnection conn;
	private final BlockingQueue<CursorAsync> cursors = new LinkedBlockingQueue<CursorAsync>();
	private final Map<String, CursorAsync> cursorsMap = Maps.of();

	public ArangoInput(String name, ArangoConnection conn) throws IOException {
		super(name);
		this.conn = conn;
		closing(this::close);
	}

	@Override
	public void open() {
		OddInput.super.open();
		if (cursors.isEmpty()) {
			if (null != conn) {
				String[] tables = conn.tables;
				List<Runnable> queries = Colls.list();
				Arrays.asList(tables).forEach(p -> {
					queries.add(() -> cursorsMap.computeIfAbsent(p, new Function<String, CursorAsync>() {
						private static final long serialVersionUID = 672656641235502988L;

						@Override
						public CursorAsync apply(String pp) {
							return new CursorAsync(p);
						}
					}));
				});
				Exeter.of().join(queries.toArray(new Runnable[queries.size()]));
			} else throw new RuntimeException("No table defined for input.");
		}
	}

	@Override
	public boolean empty() {
		return cursorsMap.isEmpty();
	}

	@Override
	public Rmap dequeue() {
		CursorAsync c;
		Map<String, Object> m;
		while (opened() && !empty())
			if (null != (c = cursors.poll())) try {
				if (c.cursor.hasNext()) {
					try {
						BaseDocument bd = c.cursor.next();
						m = bd.getProperties();
						m.put("_id", bd.getId());
						m.put("_key", bd.getKey());
					} catch (ArangoDBException ex) {
						logger.warn("ArangoDB fail fetch, ignore and continue retry: " + ex.getMessage());
						continue;
					} catch (IllegalStateException ex) {
						continue;
					}
					Object key = m.containsKey("_id") ? m.get("_id").toString() : null;
					String collection = c.col;
					Rmap msg = new Rmap(collection, key);
					m.forEach((k, v) -> {
						if (null == v) {
							logger.error("arango result field [" + k + "] null at table [" + collection + "], id [" + key + "].");
						} else {
							msg.put(k, v);
						}
					});
					return msg;
				} else {
					try {
						c.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
					c = null;
				}
			} finally {
				if (null != c) cursors.offer(c);
			}
		return null;
	}

	private class CursorAsync {
		String col;
		ArangoCursorAsync<BaseDocument> cursor;

		public CursorAsync(String col) {
			super();
			String aql = "for data in " + col + " return data";
			this.col = col;
			ArangoCursorAsync<BaseDocument> c;
			try {
				c = conn.cursor(aql).get();
				if (c.hasNext()) {
					logger.info("ArangoDB query [" + col + "] successed, count: [" + c.getCount() + "].");
					cursors.add(this);
				} else {
					logger.warn("ArangoDB query [" + col + "] finished but empty.");
					c = null;
				}
			} catch (Exception ex) {
				logger.error("ArangoDB query [" + col + "] failed", ex);
				c = null;
			}
			cursor = c;
		}

		public void close() throws IOException {
			try {
				cursor.close();
			} finally {
				cursorsMap.remove(col);
			}
		}

	}
}
