package net.butfly.albatis.bcp;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.logger.Statistic;
import net.butfly.albatis.bcp.imports.trans.TransToZIP;
import net.butfly.albatis.ddl.Qualifier;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.OutputBase;
import net.butfly.albatis.io.Rmap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import static net.butfly.albatis.bcp.Props.BCP_FLUSH_COUNT;
import static net.butfly.albatis.bcp.Props.BCP_FLUSH_MINS;

public class BcpOutput extends OutputBase<Rmap> {
    private static final long serialVersionUID = -1721490333224734613L;
    private static final ForkJoinPool EXPOOL_BCP = //
            new ForkJoinPool(Props.BCP_PARAL, ForkJoinPool.defaultForkJoinWorkerThreadFactory, null, true);
//	private static final ForkJoinPool EXPOOL_HTTP = //
//			new ForkJoinPool(Props.HTTP_PARAL, ForkJoinPool.defaultForkJoinWorkerThreadFactory, null, true);

    private final BcpFormat bcp;
    private Map<String, LinkedBlockingQueue<Rmap>> poolMap = Maps.of();
    private final PoolTask pooling;
    private List<TaskDesc> tasks = new ArrayList<>();
    private URISpec  uri;
    private final String tmpDir;
    AtomicLong totalCount = new AtomicLong();
    AtomicLong count = new AtomicLong();

    public BcpOutput(String name, URISpec uri, TableDesc... tables) throws IOException {
        super(name);
        tmpDir = uri.getParameter("tmpDir", "tmpDir");
        initBcp(tables);
        this.uri = uri;
        this.bcp = new BcpFormat(tasks);
        this.pooling = new PoolTask();
        closing(this::close);
        open();
    }

    private void initBcp(TableDesc... tables) {
        for (TableDesc tableDesc : tables) {
            TaskDesc taskDesc = new TaskDesc(tableDesc);
            tasks.add(taskDesc);
        }
    }

    @Override
    public void close() {
        pooling.stop();
    }

    @Override
    public Statistic s() {
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
            	poolMap.putIfAbsent(m.table().qualifier, new LinkedBlockingQueue<>(50000));
                LinkedBlockingQueue<Rmap> pool = poolMap.get(m.table().qualifier);
                Rmap rmap = new Rmap(new Qualifier(m.table().qualifier), m.map());
                if(null == m.keyField()) return;
                if (rmap.containsKey(m.keyField()))
                    if (null != rmap.get(m.keyField()) && Props.BCP_KEY_FIELD_EXCLUDE) {
                    	pool.put(rmap);
                    	totalCount.incrementAndGet();
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
			while (!isEmpty() || count.get() > 0 || (totalCount.get() > 0) ) {
        		try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
				}
        	}
    		flag = false;
    		poolMap.forEach((k, v) -> {
    			while (!v.isEmpty()) bcp(null);
    		});
    		EXPOOL_BCP.shutdown();
    		TransToZIP.CACHED_THREAD_POOL.shutdown();
        }

        public boolean isEmpty() {
        	return poolMap.values().stream().map(q -> q.isEmpty()).reduce((p, q) -> q && p).orElse(false);
        }
        
        private void bcp(String taskName) {
            if (null == taskName) {
                poolMap.forEach((k, v) -> {
                    while (null != v.poll()) {
                        List<Rmap> l = new ArrayList<>();
                        poolMap.get(k).drainTo(l, BCP_FLUSH_COUNT);
                        if (!l.isEmpty()) {
                        	count.incrementAndGet();
                        	EXPOOL_BCP.submit(() -> {
                        		try {
                        			bcp.bcp(l, uri, k, tmpDir);
                        		} catch (Exception e) {
                        			logger().error("BCP ERROR!", e);
                        		} finally {
                        			count.decrementAndGet();
                        			totalCount.addAndGet(l.size() * -1);
                        		}
                        	});
                        }
                    }
                });
            } else {
                List<Rmap> l = new ArrayList<>();
                poolMap.get(taskName).drainTo(l, BCP_FLUSH_COUNT);
                if (!l.isEmpty()) {
                	count.incrementAndGet();
                	EXPOOL_BCP.submit(() -> {
                		try {
                			bcp.bcp(l, uri, taskName, tmpDir);
                		} catch (Exception e) {
                			logger().error("BCP ERROR!", e);
                		} finally {
                			count.decrementAndGet();
                			totalCount.addAndGet(l.size() * -1);
                		}
                	});
                }
            }
            while (count.get() > 0) {
                try {
                    Thread.sleep(Props.BCP_WAITING_TIME);
                } catch (InterruptedException e) {
                    logger().error("Thread sleep error");
                }
            }
        }

        @Override
        public void run() {
            long last = System.currentTimeMillis();
            while (flag) {
            	long now = System.currentTimeMillis();
            	for (Map.Entry<String, LinkedBlockingQueue<Rmap>> entry : poolMap.entrySet()) {
            		if (entry.getValue().size() >= BCP_FLUSH_COUNT || now - last > ms) {
            			bcp(entry.getKey());
            			last = now;
            		} else {
            			try {
							Thread.sleep(10);
						} catch (InterruptedException e) {
            			    logger().error("Thread sleep error");
						}
            		}
            	}
            }
        }
    }
}
