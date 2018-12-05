package net.butfly.albatis.kudu;

import net.butfly.albacore.io.lambda.Consumer;
import net.butfly.albacore.io.lambda.Supplier;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.logger.Statistic;
import net.butfly.albatis.io.Input;
import net.butfly.albatis.io.Rmap;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduScanner;
import org.apache.kudu.client.RowResult;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class KuduInput extends net.butfly.albacore.base.Namedly implements Input<Rmap> {

    private static final long serialVersionUID = 2766579669550942687L;
    private final KuduConnectionBase<?, ?, ?> conn;
    private final BlockingQueue<TableScanner> scanners = new LinkedBlockingQueue<>();
    private final Map<String, TableScanner> scannerMap = Maps.of();

    public KuduInput(String name, KuduConnectionBase<?, ?, ?> conn) {
        super(name);
        this.conn = conn;
        closing(this::closeKudu);
    }

    @Override
    public void open() {
        if (scannerMap.isEmpty()) {
            if (null != conn.uri().getFile()) table(conn.uri().getFile());
        }
        Input.super.open();
    }

    private void closeKudu() {
        TableScanner s;
        while (!scannerMap.isEmpty())
            if (null != (s = scanners.poll())) s.close();
        try {
            conn.close();
        } catch (Exception e) {
        }
    }

    public void table(String table) {
        table(table, () -> new TableScanner(table));
    }

    private void table(String table, Supplier<TableScanner> constr) {
        scannerMap.compute(table, (t, existed) -> {
            if (null != existed) {
                logger().error("Table [" + table + "] input existed and conflicted, ignore new scan request.");
                return existed;
            }
            TableScanner s = constr.get();
            scanners.offer(s);
            return s;
        });
    }

    private class TableScanner {
        final String name;
        KuduScanner scanner;

        public TableScanner(String table) {
            super();
            name = table;
        }

        public void close() {
            try {
                scanner.close();
            } catch (Exception e) {
                logger().error("close kudu client exception", e);
            } finally {
                scannerMap.remove(name);
            }
        }

        public List<RowResult> next() {
            try {
                List<RowResult> rowResults = new ArrayList<>();
                scanner.nextRows().forEach(rowResults::add);
                return rowResults;
            } catch (IOException e) {
                logger().error("scanner row data failure",e);
                return null;
            }
        }
    }

    @Override
    public Statistic trace() {
        return new Statistic(this).sizing(KuduScanner::getLimit).<KuduScanner>sampling(ks -> {
            try {
                return ks.nextRows().next().rowToString();
            } catch (KuduException e) {
                logger().error("trace numRow and row data exception",e);
                return null;
            }
        });
    }

    @Override
    public boolean empty() {
        return scannerMap.isEmpty();
    }

    public Rmap result(String table) {
        return new Rmap(table,conn.schemas(table));
    }

    @Override
    public void dequeue(Consumer<Sdream<Rmap>> using) {
        TableScanner s;
        while (opened() && !empty())
            if (null != (s = scanners.poll())) {
                try {
                    List<RowResult> results = s.next();
                    if (null != results) {
                        if (results.size() > 0) {
                            List<Rmap> ms = Colls.list();
                            ms.add(result(s.name));
                            if (!ms.isEmpty()) {
                                using.accept(Sdream.of(ms));
                                return;
                            }
                        } else {
                            s.close();
                            s = null;
                        }
                    }
                } finally {
                    if (null != s) scanners.offer(s);
                }
            }
    }
}
