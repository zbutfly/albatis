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
import net.butfly.albacore.utils.logger.Loggable;
import net.butfly.albatis.io.OutputBase;
import net.butfly.albatis.io.Rmap;

public class CassandraOutput extends OutputBase<Rmap> implements Loggable {

    private static final long serialVersionUID = 5480193154210194139L;
    private final CassandraConnection caConn;
    private final String keyspace;
    private Session session;

    public CassandraOutput(String name, String keyspace, CassandraConnection caConn) {
        super(name);
        this.caConn = caConn;
        this.keyspace = keyspace;
        this.session = caConn.client.connect(this.keyspace);
        closing(this::close);
    }

    @Override
    protected void enqsafe(Sdream<Rmap> items) {
//        List<Rmap> failedList = Colls.list();
        List<Rmap> l = items.collect();
        Batch localBatch = QueryBuilder.unloggedBatch();
		l.forEach(i -> {
			localBatch.add(buildInsert(i));
		});
		ResultSetFuture rs = session.executeAsync(localBatch);
		try {
			rs.get();
		} catch (InterruptedException e) {
			logger().error("Query execute error:" + localBatch.getQueryString(), e);
		} catch (ExecutionException e) {
			if ("Batch too large".equals(e.getCause().getMessage())) {
				logger().warn(e.getMessage() + "Batch execute error, execute data one by one");
				l.forEach(i -> {
					while (true) { 
						try {
							session.execute(buildInsert(i));
							break;
						} catch (Exception ex) {
							logger().error("Single execution error" + i.toString(), ex);
						}
					}
				});
			} else logger().error("Query execute error:" + localBatch.getQueryString(), e);
		}
    }

    @Override
    public void close() {
        session.close();
    }

    private Insert buildInsert(Rmap i) {
    	Insert qb = QueryBuilder.insertInto(keyspace, i.table().name);
		i.forEach((k, v) -> qb.value(k, v));
		return qb;
    }
    
    @Override
    public URISpec target() {
        return caConn.uri();
    }

}
