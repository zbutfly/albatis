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
import net.butfly.albacore.utils.logger.Loggable;
import net.butfly.albatis.io.OutputBase;
import net.butfly.albatis.io.Rmap;

public class CassandraOutput extends OutputBase<Rmap> implements Loggable {

    private static final long serialVersionUID = 5480193154210194139L;
    private final CassandraConnection caConn;
    private final String keyspace;
    private CqlSession session;
    private static boolean BATCH_INSERT = Boolean.parseBoolean(Configs.gets("albatis.cassandra.batch.insert", "true"));
    private static final ForkJoinPool EXECUTE_POOL_CA = //
            new ForkJoinPool(Integer.parseInt(Configs.gets("albatis.cassandra.batch.insert.paral", "5")), ForkJoinPool.defaultForkJoinWorkerThreadFactory, null, true);
    private int INSERT_SIZE = Integer.parseInt(Configs.gets("albatis.cassandra.batch.insert.size", "400"));
    private long NOT_ENOUGH_WAIT = Long.parseLong(Configs.gets("albatis.cassandra.batch.notEnough.wait", "30000"));
    private int EMPTY_WAIT = Integer.parseInt(Configs.gets("albatis.cassandra.batch.insert.wait", "100"));
    private AtomicBoolean STOPPED = new AtomicBoolean(false);
    private AtomicLong count = new AtomicLong();
    private final LinkedBlockingQueue<Rmap> lbq = new LinkedBlockingQueue<>(50000);

    public CassandraOutput(String name, String keyspace, CassandraConnection caConn) {
        super(name);
        this.caConn = caConn;
        this.keyspace = keyspace;
        this.session = caConn.client;
        Thread thread = new Thread(new InsertTask());
        thread.start();
        closing(this::close);
    }

    @Override
    protected void enqsafe(Sdream<Rmap> items) {
        List<Rmap> failedList = Colls.list();
        List<Rmap> msgs = items.collect();
        if (BATCH_INSERT) {
            items.eachs(i -> {
                try {
                    lbq.put(i);
                } catch (InterruptedException e) {
                    logger().error("queue put data error" , e);
                }
            });
        } else {
            msgs.forEach(m -> {
                try {
                    upsert(m);
                } catch (Exception e) {
                    failedList.add(m);
                    logger().error("failed to insert or update items", e);
                }
            });
        }
    }

    @Override
    public void close() {
        while (!lbq.isEmpty()) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                logger().error("thread sleep error" , e);
            }
        }
        STOPPED.set(true);
        EXECUTE_POOL_CA.shutdown();
        session.close();
    }

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

    private class InsertTask implements Runnable {

        @Override
        public void run() {
            long lastTime = System.currentTimeMillis();
            while (!STOPPED.get()) {
                List<Rmap> l = Colls.list();
                long now = System.currentTimeMillis();
                if (lbq.size() > INSERT_SIZE || (lbq.size() > 0 && now - lastTime > NOT_ENOUGH_WAIT)) {
                    lastTime = now;
                    lbq.drainTo(l, INSERT_SIZE);
                    EXECUTE_POOL_CA.submit(() -> {
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
                        logger().debug("count" + count.addAndGet(l.size()));
                    });
                } else {
                    try {
                        Thread.sleep(EMPTY_WAIT);
                    } catch (InterruptedException e) {
                        logger().error("thread sleep error" , e);
                    }
                }
            }
        }

    }

    @Override
    public URISpec target() {
        return caConn.uri();
    }

}
