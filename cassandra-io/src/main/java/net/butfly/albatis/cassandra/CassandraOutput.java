package net.butfly.albatis.cassandra;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.set;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Batch;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Update;

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
	private Session session;
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
		this.session = Caconn.client.connect();
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
		Update update = QueryBuilder.update(keyspace, rmap.table().name);
		rmap.map().forEach((k, v) -> {
			update.with(set(k, v));
		});
		update.where(eq(rmap.keyField(), rmap.key()));
		ResultSet ur = session.execute(update);
		boolean applied = ur.wasApplied();
		System.out.print(ur);
		if (!applied) {
			Insert insert = QueryBuilder.insertInto(keyspace, rmap.table().name);
			rmap.map().forEach((k, v) -> {
				insert.value(k, v);
			});
			System.out.println(insert);
			ResultSet ir = session.execute(insert);
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
						Batch localBatch = QueryBuilder.unloggedBatch();
						l.forEach(i -> {
							Insert qb = QueryBuilder.insertInto(keyspace, i.table().name);
							i.forEach((k, v) -> qb.value(k, v));
							localBatch.add(qb);
						});
						ResultSetFuture rs = session.executeAsync(localBatch);
						try {
							rs.get();
							System.out.println("count" + count.addAndGet(l.size()));
						} catch (InterruptedException e) {
							e.printStackTrace();
						} catch (ExecutionException e) {
							e.printStackTrace();
						}
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
