package net.butfly.albatis.file;

import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.Configs;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albatis.ddl.FieldDesc;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.OutputBase;
import net.butfly.albatis.io.Rmap;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import static net.butfly.albacore.utils.Configs.get;

public class FileOutput extends OutputBase<Rmap> {
    private static final long serialVersionUID = 7401929118128636464L;
    private final FileConnection fc;
    private FileFormat fileFormat;
    private final PoolTask pooling;
    AtomicLong count = new AtomicLong();

    static {
        Configs.of(System.getProperty("dpcu.conf", "dpcu.properties"), "dataggr.migrate");
    }

    private static final ForkJoinPool EXPOOL_FILE =
            new ForkJoinPool(Integer.valueOf(get("file.parallelism", "50")), ForkJoinPool.defaultForkJoinWorkerThreadFactory, null, true);
    public static final int FILE_FLUSH_COUNT = Integer.parseInt(get("file.flush.count", "10000"));
    private Map<String, LinkedBlockingQueue<Rmap>> poolMap = Maps.of();

    public FileOutput(FileConnection file) {
        super("FileOutput");
        this.fc = file;
        this.fileFormat = new FileFormat(fc);
        this.pooling = new PoolTask();
        closing(this::close);
        open();
    }

    @Override
    protected void enqsafe(Sdream<Rmap> items) {
        items.eachs(m -> {
            try {
                if (poolMap.containsKey(m.table().qualifier)) {
                    LinkedBlockingQueue<Rmap> pool = poolMap.get(m.table().qualifier);
                    pool.put(m);
                } else {
                    LinkedBlockingQueue<Rmap> pool = new LinkedBlockingQueue<>(50000);
                    pool.put(m);
                    poolMap.put(m.table().qualifier, pool);
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public void close() {
        while (count.get() > 0) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        pooling.stop();
    }

    private class PoolTask implements Runnable {
        private volatile boolean flag = true;
        private final Thread thread;
        private final long ms = 5 * 60 * 1000;

        public PoolTask() {
            flag = true;
            this.thread = new Thread(this);
            this.thread.setName("BcpPoolingThread");
            this.thread.start();
        }

        public void stop() {
            flag = false;
            poolMap.forEach((k, v) -> {
                if (!v.isEmpty()) {
                    for (TableDesc tableDesc : fc.tables) {
                        if (k.equals(tableDesc.qualifier.name)) {
                            bcp(k, tableDesc.fieldDescList);
                        }
                    }
                }
            });
        }

        private void bcp(String tableName, List<FieldDesc> fieldDescs) {
            List<Rmap> l = new ArrayList<>();
            poolMap.get(tableName).drainTo(l, FILE_FLUSH_COUNT);
            count.incrementAndGet();
            EXPOOL_FILE.submit(() -> {
                fileFormat.format(l, tableName, fieldDescs);
                count.decrementAndGet();
            });
        }

        @Override
        public void run() {
            long last = System.currentTimeMillis();
            while (flag) {
                for (Map.Entry<String, LinkedBlockingQueue<Rmap>> entry : poolMap.entrySet()) {
                    if (entry.getValue().size() >= FILE_FLUSH_COUNT || System.currentTimeMillis() - last > ms) {
                        for (TableDesc tableDesc : fc.tables) {
                            if (entry.getKey().equals(tableDesc.qualifier.name)) {
                                bcp(entry.getKey(), tableDesc.fieldDescList);
                                last = System.currentTimeMillis();
                            }
                        }
                    }
                }
            }
        }
    }
}
