package net.butfly.albatis.hmpp;

import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.OutputBase;
import net.butfly.albatis.io.Rmap;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicLong;

import static net.butfly.albacore.utils.Configs.get;

public class HmppOutput extends OutputBase<Rmap> {
    static final Logger logger = Logger.getLogger(HmppOutput.class);
    HmppConnection conn;
    private Map<String, LinkedBlockingQueue<Rmap>> poolMap = Maps.of();
    private final PoolTask pooling;
    public static final int HMPP_FLUSH_COUNT = Integer.parseInt(get("hmpp.flush.count", "10000"));
    public static final int HMPP_POOL_SIZE = Integer.parseInt(get("hmpp.pool.size", "50000"));
    private static final ForkJoinPool EXPOOL =
            new ForkJoinPool(Integer.valueOf(get("hmpp.parallelism", "50")), ForkJoinPool.defaultForkJoinWorkerThreadFactory, null, true);
    AtomicLong count = new AtomicLong();
    HmppUpload hmppUpload = new HmppUpload();

    public HmppOutput(HmppConnection hmppConnection) {
        super("HmppOutput");
        this.conn = hmppConnection;
        this.pooling = new PoolTask();
        closing(this::close);
        open();
    }

    @Override
    public void close() {
        pooling.stop();
        while (count.get() > 0) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    protected void enqsafe(Sdream<Rmap> sdream) {
        synchronized (this) {
            sdream.eachs(m -> {
                try {
                    if (poolMap.containsKey(m.table().qualifier)) {
                        LinkedBlockingQueue<Rmap> pool = poolMap.get(m.table().qualifier);
                        pool.put(m);
                        poolMap.put(m.table().qualifier, pool);
                    } else {
                        LinkedBlockingQueue<Rmap> pool = new LinkedBlockingQueue<>(HMPP_POOL_SIZE);
                        pool.put(m);
                        poolMap.put(m.table().qualifier, pool);
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }


    private class PoolTask implements Runnable {
        private volatile boolean flag = true;
        private final Thread thread;
        private final long ms = 1 * 60 * 1000;

        public PoolTask() {
            flag = true;
            this.thread = new Thread(this);
            this.thread.setName("FilePoolingThread");
            this.thread.start();
        }

        public void stop() {
            flag = false;
            try {
                for (Map.Entry<String, LinkedBlockingQueue<Rmap>> entry : poolMap.entrySet()) {
                    for (TableDesc tableDesc : conn.tables) {
                        if (entry.getKey().equals(tableDesc.qualifier.name)) {
                            fileFormat(entry.getKey());
                        }
                    }
                }
            } catch (RejectedExecutionException re) {
            } catch (Exception e) {
                logger.error("PoolTaskThead error:" + e);
            }
        }

        @Override
        public void run() {
            long last = System.currentTimeMillis();
            while (flag) {
                try {
                    for (Map.Entry<String, LinkedBlockingQueue<Rmap>> entry : poolMap.entrySet()) {
                        if (entry.getValue().size() >= HMPP_FLUSH_COUNT || System.currentTimeMillis() - last > ms) {
                            for (TableDesc tableDesc : conn.tables) {
                                if (entry.getKey().equals(tableDesc.qualifier.name)) {
                                    fileFormat(entry.getKey());
                                    last = System.currentTimeMillis();
                                }
                            }
                        }
                    }
                } catch (RejectedExecutionException re) {
                } catch (Exception e) {
                    logger.error("PoolTaskThead error:" + e);
                }
            }
        }

        private void fileFormat(String tableName) {
            List<Rmap> l = new ArrayList<>();
            poolMap.get(tableName).drainTo(l, HMPP_FLUSH_COUNT);
            count.incrementAndGet();
            EXPOOL.submit(() -> {
                hmppUpload.upload(l, tableName, conn.connInfo);
                count.decrementAndGet();
            });
        }
    }
}
