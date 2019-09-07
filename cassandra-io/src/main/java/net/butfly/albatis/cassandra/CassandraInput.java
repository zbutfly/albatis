package net.butfly.albatis.cassandra;

import java.util.Iterator;
import java.util.Objects;

import com.datastax.oss.driver.api.core.cql.*;


import net.butfly.albacore.utils.logger.Loggable;
import net.butfly.albatis.ddl.Qualifier;
import net.butfly.albatis.io.OddInput;
import net.butfly.albatis.io.Rmap;

public class CassandraInput extends net.butfly.albacore.base.Namedly implements OddInput<Rmap>, Loggable {

    private static final long serialVersionUID = -4789851501849429707L;
    private final CassandraConnection caConn;
    private String tableName;
    private ResultSet rs;
    private Iterator<Row> it;

    public CassandraInput(String name, CassandraConnection caConn) {
        super(name);
        this.caConn = caConn;
        closing(this::closeCassandra);
    }

    public CassandraInput(final String name, CassandraConnection caConn, String tableName) {
        super(name);
        this.tableName = tableName;
        this.caConn = caConn;
        openCassandra();
        closing(this::closeCassandra);
    }

    @Override
    public Rmap dequeue() {
        Row row = null;
        Rmap msg;
        synchronized (it) {
            if (!it.hasNext() || null == (row = it.next())) return null;
        }
        msg = new Rmap(new Qualifier(tableName));
        for (ColumnDefinition key : row.getColumnDefinitions())
            msg.put(key.getName().toString(), Objects.requireNonNull(row.getObject(key.getName())));
        return msg;
    }

    private void openCassandra() {
        try {
            String cql = "select * from " + caConn.defaultKeyspace + "." + tableName;
            rs = caConn.client.execute(cql);
            it = rs.iterator();
        } catch (Exception e) {
            logger().error("open cassandra error", e);
        }
        rs = null;
    }

    private void closeCassandra() {
        try {
            caConn.close();
        } catch (Exception e) {
            logger().error("close cassandra error", e);
        }
    }
}
