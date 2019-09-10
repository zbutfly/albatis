package net.butfly.albatis.cassandra;

import java.util.Iterator;

import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;

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
    private boolean next;

    public CassandraInput(String name, CassandraConnection caConn) {
        super(name);
        this.caConn = caConn;
        closing(this::close);
    }

    public CassandraInput(final String name, CassandraConnection caConn, String tableName) {
        super(name);
        this.tableName = tableName;
        this.caConn = caConn;
        openCassandra();
        closing(this::close);
    }

    @Override
    public Rmap dequeue() {
        Row row = null;
        Rmap msg;
        synchronized (it) {
            if (it.hasNext())  {
            	if (null == (row = it.next())) return null;
            } else {
            	next = false;
            	return null;
            }
        }
        msg = new Rmap(new Qualifier(tableName));
        for (ColumnDefinition key : row.getColumnDefinitions()) {
        	Object o = row.getObject(key.getName());
        	if (null != o) 
        		msg.put(key.getName().toString(), o);
        }
        return msg;
    }

    private void openCassandra() {
        try {
            String cql = "select * from " + caConn.defaultKeyspace + "." + tableName;
            rs = caConn.client.execute(cql);
            it = rs.iterator();
            next = it.hasNext();
        } catch (Exception e) {
            throw new RuntimeException("Open cassandra error", e);
        }
        rs = null;
    }

    @Override
    public boolean empty() {
    	return !next;
    }

    @Override
    public void close() {
        try {
            caConn.close();
        } catch (Exception e) {
            logger().error("close cassandra error", e);
        }
    }
}
