package net.butfly.albatis.bcp;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.logger.Statistic;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.OutputBase;
import net.butfly.albatis.io.Rmap;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import static net.butfly.albacore.utils.Configs.get;
import static net.butfly.albatis.bcp.Props.BCP_FLUSH_COUNT;
import static net.butfly.albatis.bcp.Props.BCP_FLUSH_MINS;

public class BcpOutput extends OutputBase<Rmap> {
    private static final long serialVersionUID = -1721490333224734613L;
    private static final ForkJoinPool EXPOOL_BCP = //
            new ForkJoinPool(Props.BCP_PARAL, ForkJoinPool.defaultForkJoinWorkerThreadFactory, null, true);
//	private static final ForkJoinPool EXPOOL_HTTP = //
//			new ForkJoinPool(Props.HTTP_PARAL, ForkJoinPool.defaultForkJoinWorkerThreadFactory, null, true);

    private final BcpFormat bcp;
    //    private final LinkedBlockingQueue<Map<String, Object>> pool = new LinkedBlockingQueue<>(50000);// data
    private Map<String, LinkedBlockingQueue<Rmap>> poolMap = Maps.of();
    private final PoolTask pooling;
    private boolean inc;
    private List<TaskDesc> tasks = new ArrayList<>();
    private URISpec uriSpec, uri;
    AtomicLong count = new AtomicLong();

    public BcpOutput(String name, URISpec uri, TableDesc... tables) throws IOException {
        super(name);
        initBcp(tables);
        this.uri = uri;
        this.bcp = new BcpFormat(tasks);
        this.pooling = new PoolTask();
        closing(this::close);
        open();
    }

    private void initBcp(TableDesc... tables) {
        for (TableDesc tableDesc : tables) {
            inc = Boolean.parseBoolean(get("input.inc", Boolean.toString(null != tableDesc.qualifier.name && tableDesc.qualifier.name.endsWith("_inc"))));
            uriSpec = new URISpec(get("config"));
            try (Config ds = new Config(uriSpec.toString(), uriSpec.getParameter("user"), uriSpec.getParameter("password"))) {
                List<TaskDesc> taskDescList = inc ? ds.getTaskConfigInc(tableDesc.qualifier.name) : ds.getTaskConfig(tableDesc.qualifier.name);
                if (taskDescList.isEmpty()) throw new IllegalArgumentException("Task not found");
                TaskDesc task = taskDescList.get(0);
                task.fields.addAll(inc ? ds.getTaskStatisticsInc(task.id) : ds.getTaskStatistics(task.id));
                tasks.add(task);
            } catch (SQLException e) {
                throw new IllegalArgumentException(e);
            }
        }
    }

    @Override
    public void close() {
        pooling.stop();
    }

    @Override
    public Statistic s() {
//        return super.s().detailing(() -> "Pending records: " + pool.size() + ", thread pool: " + EXPOOL_BCP.toString());
        Statistic statistic = new Statistic(BcpOutput.class);
        for (Map.Entry<String, LinkedBlockingQueue<Rmap>> entry : poolMap.entrySet()) {
            statistic = super.s().detailing(() -> "Pending table :" + entry.getKey() + ",Pending records: " + entry.getValue().size() + ", thread pool: " + EXPOOL_BCP.toString());
        }
        return statistic;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void enqsafe(Sdream<Rmap> msgs) {
        msgs.eachs(m -> {
            try {
                if (poolMap.containsKey(m.table().qualifier)) {
                    LinkedBlockingQueue<Rmap> pool = poolMap.get(m.table().qualifier);
                    Map<String, Object> map = m.containsKey("value") ? (Map<String, Object>) m.get("value") : m.map();
                    Rmap rmap = new Rmap(m.table().qualifier, map);
                    if(null == m.keyField()) return;
                    if (rmap.containsKey(m.keyField()))
                        if (null != rmap.get(m.keyField()) && Props.BCP_KEY_FIELD_EXCLUDE)
                            pool.put(rmap);
                } else {
                    LinkedBlockingQueue<Rmap> pool = new LinkedBlockingQueue<>(50000);
                    Map<String, Object> map = m.containsKey("value") ? (Map<String, Object>) m.get("value") : m.map();
                    Rmap rmap = new Rmap(m.table().qualifier, map);
                    if(null == m.keyField()) return;
                    if (rmap.containsKey(m.keyField()))
                        if (null != rmap.get(m.keyField()) && Props.BCP_KEY_FIELD_EXCLUDE) {
                            pool.put(rmap);
                            poolMap.put(m.table().qualifier, pool);
                        }
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private class PoolTask implements Runnable {
        private volatile boolean flag = true;
        private final long ms = BCP_FLUSH_MINS * 60 * 1000;
        private final Thread thread;

        public PoolTask() {
            flag = true;
            this.thread = new Thread(this);
            this.thread.setName("BcpPoolingThread");
            this.thread.start();
        }

        public void stop() {
            flag = false;
            poolMap.forEach((k, v) -> {
                while (!v.isEmpty()) bcp(null);
            });
        }

        private void bcp(String taskName) {
            if (null == taskName) {
                poolMap.forEach((k, v) -> {
                    while (null != v.poll()) {
                        List<Rmap> l = new ArrayList<>();
                        poolMap.get(k).drainTo(l, BCP_FLUSH_COUNT);
                        count.incrementAndGet();
                        EXPOOL_BCP.submit(() -> {
                            bcp.bcp(l, uri, k);
                            count.decrementAndGet();
                        });
                    }
                });
            } else {
                List<Rmap> l = new ArrayList<>();
                poolMap.get(taskName).drainTo(l, BCP_FLUSH_COUNT);
                count.incrementAndGet();
                EXPOOL_BCP.submit(() -> {
                    bcp.bcp(l, uri, taskName);
                    count.decrementAndGet();
                });
            }
            while (count.get() > 0) {
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        @Override
        public void run() {
            long last = System.currentTimeMillis();
            while (flag)
                for (Map.Entry<String, LinkedBlockingQueue<Rmap>> entry : poolMap.entrySet()) {
                    if (entry.getValue().size() >= BCP_FLUSH_COUNT || System.currentTimeMillis() - last > ms) {
                        bcp(entry.getKey());
                    }
                }
        }
    }
}
