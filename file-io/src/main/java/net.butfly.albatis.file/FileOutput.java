package net.butfly.albatis.file;

import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.Configs;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.ddl.FieldDesc;
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

public class FileOutput extends OutputBase<Rmap> {
    private static final long serialVersionUID = 7401929118128636464L;
    public static Logger logger = Logger.getLogger(FileOutput.class);
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
    public static final int FILE_FLUSH_MINS = Integer.parseInt(get("file.flush.idle.minutes", "5"));
    private Map<String, LinkedBlockingQueue<Rmap>> poolMap = Maps.of();
    private static final AtomicLong COUNT = new AtomicLong();

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
        synchronized (this){
            items.eachs(m -> {
                try {
                    COUNT.incrementAndGet();
                    if (poolMap.containsKey(m.table().qualifier)) {
                        LinkedBlockingQueue<Rmap> pool = poolMap.get(m.table().qualifier);
                        pool.put(m);
                        poolMap.put(m.table().qualifier, pool);
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
    }

    @Override
    public void close() {
        logger.info("total count:"+COUNT);
        pooling.stop();
        while (count.get() > 0) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private class PoolTask implements Runnable {
        private volatile boolean flag = true;
        private final Thread thread;
        private final long ms = FILE_FLUSH_MINS * 60 * 1000;

        public PoolTask() {
            flag = true;
            this.thread = new Thread(this);
            this.thread.setName("FilePoolingThread");
            this.thread.start();
        }

        public void stop() {
            flag = false;
            poolMap.forEach((k, v) -> {
                if (!v.isEmpty()) {
                    for (TableDesc tableDesc : fc.tables) {
                        if (k.equals(tableDesc.qualifier.name)) {
                            fileFormat(k, tableDesc.fieldDescList);
                        }
                    }
                }
            });
        }

        private void fileFormat(String tableName, List<FieldDesc> fieldDescs) {
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
                try{
                    for (Map.Entry<String, LinkedBlockingQueue<Rmap>> entry : poolMap.entrySet()) {
                        if (entry.getValue().size() >= FILE_FLUSH_COUNT || System.currentTimeMillis() - last > ms) {
                            for (TableDesc tableDesc : fc.tables) {
                                if (entry.getKey().equals(tableDesc.qualifier.name)) {
                                    fileFormat(entry.getKey(), tableDesc.fieldDescList);
                                    last = System.currentTimeMillis();
                                }
                            }
                        }
                    }
                }catch (RejectedExecutionException re){
                } catch (Exception e) {
                    logger.error("PoolTaskThead error:"+e);
                }
            }
        }
    }
}
