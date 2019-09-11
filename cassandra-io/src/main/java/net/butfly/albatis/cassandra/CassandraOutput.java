package net.butfly.albatis.cassandra;

import java.util.List;
import java.util.concurrent.ExecutionException;

import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Batch;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.paral.Sdream;
//import net.butfly.albacore.utils.Configs;
import net.butfly.albacore.utils.logger.Loggable;
import net.butfly.albatis.io.OutputBase;
import net.butfly.albatis.io.Rmap;

public class CassandraOutput extends OutputBase<Rmap> implements Loggable {

    private static final long serialVersionUID = 5480193154210194139L;
    private final CassandraConnection caConn;
    private final String keyspace;
    private Session session;
//    private final int QUERY_TIMEOUT = Integer.parseInt(Configs.gets("albatis.cassandra.query.time.out", "2"));
//    private AtomicLong count = new AtomicLong();
//    private final LinkedBlockingQueue<Rmap> lbq = new LinkedBlockingQueue<>(50000);

    public CassandraOutput(String name, String keyspace, CassandraConnection caConn) {
        super(name);
        this.caConn = caConn;
        this.keyspace = keyspace;
        this.session = caConn.client.connect(this.keyspace);
//        Thread thread = new Thread(new InsertTask());
//        thread.start();
        closing(this::close);
    }

    @Override
    protected void enqsafe(Sdream<Rmap> items) {
//        List<Rmap> failedList = Colls.list();
        List<Rmap> l = items.collect();
        Batch localBatch = QueryBuilder.unloggedBatch();
		l.forEach(i -> {
			Insert qb = QueryBuilder.insertInto(keyspace, i.table().name);
			i.forEach((k, v) -> qb.value(k, v));
			localBatch.add(qb);
		});
		ResultSetFuture rs = session.executeAsync(localBatch);
		try {
			rs.get();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}
    }

    @Override
    public void close() {
        session.close();
    }

//    @SuppressWarnings("unused")
//	private void upsert(Rmap rmap) {
//        Update updateBuilder = QueryBuilder.update(keyspace, rmap.table().name);
//        List<Assignment> assignmentList = new ArrayList<>();
//        for (String k : rmap.keySet()) assignmentList.add(Assignment.setColumn(k, literal(rmap.get(k))));
//        SimpleStatement statement = updateBuilder.set(assignmentList).whereColumn(rmap.keyField()).isEqualTo(literal(rmap.key())).build();
//        ResultSet ur = session.execute(statement);
//        boolean applied = ur.wasApplied();
//        System.out.print(ur);
//        if (!applied) {
//            InsertInto insert = QueryBuilder.insertInto(keyspace, rmap.table().name);
//            for (String k : rmap.keySet()) insert.value(k, literal(rmap.get(k)));
//            ResultSet rs = session.execute(insert.value(rmap.keyField(), literal(rmap.key())).build());
//            logger().debug("resultSet is:" + rs);
//        }
//    }

    @Override
    public URISpec target() {
        return caConn.uri();
    }

}
