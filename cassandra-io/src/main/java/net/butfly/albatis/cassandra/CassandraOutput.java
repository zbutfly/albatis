package net.butfly.albatis.cassandra;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.DefaultBatchType;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.insert.InsertInto;
import com.datastax.oss.driver.api.querybuilder.update.Assignment;
import com.datastax.oss.driver.api.querybuilder.update.UpdateStart;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.logger.Loggable;
import net.butfly.albatis.io.OutputBase;
import net.butfly.albatis.io.Rmap;

public class CassandraOutput extends OutputBase<Rmap> implements Loggable {

    private static final long serialVersionUID = 5480193154210194139L;
    private final CassandraConnection caConn;
    private final String keyspace;
    private CqlSession session;
//    private AtomicLong count = new AtomicLong();
//    private final LinkedBlockingQueue<Rmap> lbq = new LinkedBlockingQueue<>(50000);

    public CassandraOutput(String name, String keyspace, CassandraConnection caConn) {
        super(name);
        this.caConn = caConn;
        this.keyspace = keyspace;
        this.session = caConn.client;
//        Thread thread = new Thread(new InsertTask());
//        thread.start();
        closing(this::close);
    }

    @Override
    protected void enqsafe(Sdream<Rmap> items) {
//        List<Rmap> failedList = Colls.list();
        List<Rmap> l = items.collect();
        BatchStatement batch = BatchStatement.newInstance(DefaultBatchType.UNLOGGED, l.stream().map(r -> {
            InsertInto insert = QueryBuilder.insertInto(keyspace, r.table().name);
            return insert.values(r.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> literal(e.getValue())))).build();
        }).collect(Collectors.toList()));
        CompletionStage<AsyncResultSet> cs = session.executeAsync(batch);
        try {
            cs.toCompletableFuture().get();
        } catch (InterruptedException | ExecutionException e) {
            logger().error(e.getMessage(), e);
        }
    }

    @Override
    public void close() {
        session.close();
    }

    @SuppressWarnings("unused")
	private void upsert(Rmap rmap) {
        UpdateStart updateBuilder = QueryBuilder.update(keyspace, rmap.table().name);
        List<Assignment> assignmentList = new ArrayList<>();
        for (String k : rmap.keySet()) assignmentList.add(Assignment.setColumn(k, literal(rmap.get(k))));
        SimpleStatement statement = updateBuilder.set(assignmentList).whereColumn(rmap.keyField()).isEqualTo(literal(rmap.key())).build();
        ResultSet ur = session.execute(statement);
        boolean applied = ur.wasApplied();
        System.out.print(ur);
        if (!applied) {
            InsertInto insert = QueryBuilder.insertInto(keyspace, rmap.table().name);
            for (String k : rmap.keySet()) insert.value(k, literal(rmap.get(k)));
            ResultSet rs = session.execute(insert.value(rmap.keyField(), literal(rmap.key())).build());
            logger().debug("resultSet is:" + rs);
        }
    }

    @Override
    public URISpec target() {
        return caConn.uri();
    }

}
