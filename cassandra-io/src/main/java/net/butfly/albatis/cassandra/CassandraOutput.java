package net.butfly.albatis.cassandra;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.DefaultBatchType;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.insert.InsertInto;
import com.datastax.oss.driver.api.querybuilder.update.Assignment;
import com.datastax.oss.driver.api.querybuilder.update.UpdateStart;
import com.datastax.oss.driver.api.querybuilder.term.Term;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.Configs;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albatis.io.OutputBase;
import net.butfly.albatis.io.Rmap;

public class CassandraOutput extends OutputBase<Rmap> {

	private static final long serialVersionUID = 5480193154210194139L;
	private final CassandraConnection Caconn;
	private final String keyspace;
//	private final String table_name;
	private CqlSession session;
	public static boolean BATCH_INSERT = Boolean.parseBoolean(Configs.gets("albatis.cassandra.batch.insert", "true"));
	private static final ForkJoinPool EXPOOL_CA = //
            new ForkJoinPool(Integer.parseInt(Configs.gets("albatis.cassandra.batch.insert.paral", "5")), ForkJoinPool.defaultForkJoinWorkerThreadFactory, null, true);
	private int INSERT_SIZE = Integer.parseInt(Configs.gets("albatis.cassandra.batch.insert.size", "400"));
	private long NOTENOUGH_WAIT = Long.parseLong(Configs.gets("albatis.cassandra.batch.notEnough.wait", "30000"));
	private int EMPTY_WAIT = Integer.parseInt(Configs.gets("albatis.cassandra.batch.insert.wait", "100"));
	private AtomicBoolean STOPPED = new AtomicBoolean(false);
	private AtomicLong count = new AtomicLong();
	public final LinkedBlockingQueue<Rmap> lbq = new LinkedBlockingQueue<>(50000);

	public CassandraOutput(String name, String keyspace, String table_name, CassandraConnection Caconn) {
		super(name);
		this.Caconn = Caconn;
		this.keyspace = keyspace;
//		this.table_name = table_name;
		this.session = Caconn.client;
		Thread thread = new Thread(new InsertTask());
		thread.start();
		closing(this::close);
	}

	public void createTable(String space_name, String table_name, Session session) {
		StringBuilder cql = new StringBuilder();
		cql.append("CREATE TABLE if not exists ").append(space_name).append(".").append(table_name).append("(");
	}

	@Override
	protected void enqsafe(Sdream<Rmap> items) {
		List<Rmap> failedList = Colls.list();
		List<Rmap> msgs = items.collect();
		if (BATCH_INSERT) {
//			Batch localBatch = QueryBuilder.unloggedBatch();
//			items.map(i -> {
//				Insert qb = QueryBuilder.insertInto(keyspace, i.table().name);
//				i.forEach((k, v) -> qb.value(k, v));
//				localBatch.add(qb);
//				return qb;
//			}).collect();
//			ResultSetFuture rs = session.executeAsync(localBatch);
//			try {
//				rs.get();
//			} catch (InterruptedException e) {
//				e.printStackTrace();
//			} catch (ExecutionException e) {
//				e.printStackTrace();
//			}
			items.eachs(i -> {
				try {
					lbq.put(i);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			});
//			System.out.print(rs);
		} else {
			msgs.forEach(m -> {try {
				upsert(m);	
			} catch (Exception e) {
				failedList.add(m);
				logger().error("failed to insert or update items.", e);
			}});
		}
	}

	@Override
	public void close() {
		while (!lbq.isEmpty()) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
			}
		}
		STOPPED.set(true);
		EXPOOL_CA.shutdown();
		session.close();
	}
	
	public void upsert(Rmap rmap) {
		UpdateStart updateBuilder = QueryBuilder.update(keyspace, rmap.table().name);
		List<Assignment> assignmentList = new ArrayList<>();
		for (String k : rmap.keySet()) {
			assignmentList.add(Assignment.setColumn(k, literal(rmap.get(k))));
		}
		SimpleStatement statement = updateBuilder.set(assignmentList).whereColumn(rmap.keyField()).isEqualTo(literal(rmap.key())).build();
		ResultSet ur = session.execute(statement);
		boolean applied = ur.wasApplied();
		System.out.print(ur);
		if (!applied) {
			InsertInto insert = QueryBuilder.insertInto(keyspace, rmap.table().name);
			for (String k : rmap.keySet()) {
				insert.value(k, literal(rmap.get(k)));
			}
//			System.out.println(insert);
			ResultSet ir = session.execute(insert.value(rmap.keyField(), literal(rmap.key())).build());
			System.out.print(ir);
		}
	}

	private class InsertTask implements Runnable {

		@Override
		public void run() {
			long lastTime = System.currentTimeMillis();
			while (!STOPPED.get()) {
				List<Rmap> l = Colls.list();
				long now = System.currentTimeMillis();
				if (lbq.size() > INSERT_SIZE || (lbq.size() > 0 && now - lastTime > NOTENOUGH_WAIT)) {
					lastTime = now;
					lbq.drainTo(l, INSERT_SIZE);
					EXPOOL_CA.submit(() -> {
						BatchStatement batch = BatchStatement.newInstance(DefaultBatchType.UNLOGGED, l.stream().map(r -> {
							InsertInto insert = QueryBuilder.insertInto(keyspace, r.table().name);
							return insert.values(r.entrySet().stream().collect(Collectors.toMap(e -> e.getKey(), e -> literal(e.getValue())))).build();
						}).collect(Collectors.toList()));
						CompletionStage<AsyncResultSet> cs = session.executeAsync(batch);
						try {
							cs.toCompletableFuture().get();
						} catch (InterruptedException e) {
							logger().error(e.getMessage(), e);
						} catch (ExecutionException e) {
							logger().error(e.getMessage(), e);
						}
//						cs.whenComplete((rs, err) -> {
//							if (err != null) {
//								err.printStackTrace();
//							}
//						});
						System.out.println("count" + count.addAndGet(l.size()));
					});
				} else {
					try {
						Thread.sleep(EMPTY_WAIT);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
		}
		
	}
	
	@Override
	public URISpec target() {
		return Caconn.uri();
	}

}
