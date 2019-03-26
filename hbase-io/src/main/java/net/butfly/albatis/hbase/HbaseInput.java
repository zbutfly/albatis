package net.butfly.albatis.hbase;

import static net.butfly.albacore.paral.Sdream.of;
import static net.butfly.albatis.hbase.HbaseConnection.SPLIT_BY_FAMILY;
import static net.butfly.albatis.hbase.HbaseConnection.SPLIT_BY_PREFIX;
import static net.butfly.albatis.hbase.utils.HbaseScan.Range.range;
import static net.butfly.albatis.io.IOProps.prop;
import static net.butfly.albatis.io.IOProps.propB;
import static net.butfly.albatis.io.IOProps.propI;
import static net.butfly.albatis.io.IOProps.propL;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import net.butfly.albacore.base.Namedly;
import net.butfly.albacore.io.lambda.Consumer;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.Pair;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albacore.utils.logger.Statistic;
import net.butfly.albatis.hbase.utils.HbaseSkip.SkipMode;
import net.butfly.albatis.io.Input;
import net.butfly.albatis.io.Rmap;

public class HbaseInput extends Namedly implements Input<Rmap> {
	private static final long serialVersionUID = 6225222417568739808L;
	final static Logger logger = Logger.getLogger(HbaseInput.class);
	static final long SCAN_BYTES = propL(HbaseInput.class, "scan.bytes", 1048576, "Hbase Scan.setMaxResultSize(bytes)."); // 1M
	static final int SCAN_COLS = propI(HbaseInput.class, "scan.cols.per.row", -1, "Hbase Scan.setBatch(cols per rpc).");
	static final boolean SCAN_CACHE_BLOCKS = propB(HbaseInput.class, "scan.cache.blocks", false, "Hbase Scan.setCacheBlocks(false).");
	static final int SCAN_MAX_CELLS_PER_ROW = propI(HbaseInput.class, "scan.max.cells.per.row", 10000,
			"Hbase max cells per row (more will be ignore).");
	static final Pair<SkipMode, String> SCAN_SKIP = SkipMode.parse(prop(HbaseInput.class, "scan.skip", null,
			"Hbase scan skip (for debug or resume): ROWS:n, REGIONS:n, [ROWKEY:]n."));
	final HbaseConnection hconn;
	final BlockingQueue<TableScaner> SCAN_POOL = new LinkedBlockingQueue<>();
	final Map<String, List<TableScaner>> SCAN_REGS = Maps.of();

	public HbaseInput(String name, HbaseConnection conn) {
		super(name);
		hconn = conn;
		closing(this::closeHbase);
	}

	@Override
	public void open() {
		if (SCAN_REGS.isEmpty() && null != hconn.uri().getFile()) try {
			table(hconn.uri().getFile());
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		Input.super.open();
	}

	private void closeHbase() {
		TableScaner s;
		while (!SCAN_REGS.isEmpty()) if (null != (s = SCAN_POOL.poll())) try {
			s.close();
		} catch (Exception e) {}
		try {
			hconn.close();
		} catch (Exception e) {}
	}

	@Override
	public void dequeue(Consumer<Sdream<Rmap>> using) {
		TableScaner s;
		// rowkey -> record, fetch and fullfil so that earch in the poll should be whole.
		Map<String, Rmap> wholes = Maps.of();
		while (opened() && !empty()) if (null != (s = SCAN_POOL.poll())) try {
			if (s.dequeue(wholes)) {
				s.close();
				s = null;
			}
		} finally {
			if (null != s) SCAN_POOL.offer(s);
		}
		if (!Colls.empty(wholes)) {
			Collection<Rmap> ms = wholes.values();
			if (HbaseConnection.SPLIT_ENABLED) ms = Colls.flat(Colls.list(wholes.values(), //
					r -> r.split(SPLIT_BY_FAMILY, SPLIT_BY_PREFIX)));
			using.accept(of(ms));
		}
	}

	public void table(String table) throws IOException {
		new TableScaner(this, table, null, null, null, range());
	}

	public final void table(String... table) throws IOException {
		for (String t : table)
			table(t, t);
	}

	public void table(String table, Collection<String> families, Collection<String> prefixes, byte[]... startAndStopRow) throws IOException {
		new TableScaner(this, table, families, prefixes, null, range(startAndStopRow));
	}

	@Override
	public Statistic trace() {
		return new Statistic(this).sizing(Result::getTotalSizeOfCells).<Result> sampling(r -> Bytes.toString(r.getRow()));
	}

	@Override
	public boolean empty() {
		return SCAN_REGS.isEmpty();
	}

	public static void main(String[] args) throws InterruptedException {
		System.err.println(HbaseInput.SCAN_MAX_CELLS_PER_ROW);
		// while (true)
		// Thread.sleep(10000);
	}
}
