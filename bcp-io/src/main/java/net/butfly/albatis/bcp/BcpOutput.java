package net.butfly.albatis.bcp;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.logger.Statistic;
import net.butfly.albatis.io.OutputBase;
import net.butfly.albatis.io.Rmap;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.LinkedBlockingQueue;

import static net.butfly.albacore.utils.Configs.get;
import static net.butfly.albacore.utils.Configs.logger;
import static net.butfly.albatis.bcp.Props.BCP_FLUSH_COUNT;
import static net.butfly.albatis.bcp.Props.BCP_FLUSH_MINS;

public class BcpOutput extends OutputBase<Rmap> {
    private static final long serialVersionUID = -1721490333224734613L;
    private static final ForkJoinPool EXPOOL_BCP = //
            new ForkJoinPool(Props.BCP_PARAL, ForkJoinPool.defaultForkJoinWorkerThreadFactory, null, true);
//	private static final ForkJoinPool EXPOOL_HTTP = //
//			new ForkJoinPool(Props.HTTP_PARAL, ForkJoinPool.defaultForkJoinWorkerThreadFactory, null, true);

    private final BcpFormat bcp;
    private final LinkedBlockingQueue<Map<String, Object>> pool = new LinkedBlockingQueue<>(50000);// data
    private final PoolTask pooling;
    private boolean inc;
    private List<TaskDesc> tasks;
    private TaskDesc task;
    private URISpec uriSpec,uri;

    public BcpOutput(String name,URISpec uri, String taskName) throws IOException {
        super(name);
        initBcp(taskName);
        this.uri = uri;
        this.bcp = new BcpFormat(task);
        this.pooling = new PoolTask();
        closing(this::close);
        open();
    }

    private void initBcp(String taskname) {
        inc = Boolean.parseBoolean(get("input.inc", Boolean.toString(null != taskname && taskname.endsWith("_inc"))));
        uriSpec = new URISpec(get("config"));
        try (Config ds = new Config(uriSpec.toString(), uriSpec.getParameter("user"), uriSpec.getParameter("password"))) {
            tasks = inc ? ds.getTaskConfigInc(taskname) : ds.getTaskConfig(taskname);
            if (tasks.isEmpty()) throw new IllegalArgumentException("Task not found");
            task = tasks.get(0);
            if (tasks.size() > 1)
                logger.warn("Only first task supported, but [" + tasks.size() + "] tasks found in config db. Other is INGORED.");
            task.fields.addAll(inc ? ds.getTaskStatisticsInc(task.id) : ds.getTaskStatistics(task.id));
        } catch (SQLException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public void close() {
        pooling.stop();
    }

    @Override
    public Statistic trace() {
        return super.trace().detailing(() -> "Pending records: " + pool.size() + ", thread pool: " + EXPOOL_BCP.toString());
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void enqsafe(Sdream<Rmap> msgs) {
        msgs.eachs(m -> {
            try {
                pool.put(s().stats(m.containsKey("value") ? (Map<String, Object>) m.get("value") : m.map()));
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
            while (!pool.isEmpty()) bcp();
        }

        private void bcp() {
            List<Map<String, Object>> l = new ArrayList<>();
            pool.drainTo(l, BCP_FLUSH_COUNT);
            EXPOOL_BCP.submit(() -> {
                bcp.bcp(l,uri);
            });
        }

        @Override
        public void run() {
            long last = -1;
            while (flag) if (pool.size() >= BCP_FLUSH_COUNT || System.currentTimeMillis() - last > ms) {
                last = System.currentTimeMillis();
                bcp();
            }
        }
    }
}
