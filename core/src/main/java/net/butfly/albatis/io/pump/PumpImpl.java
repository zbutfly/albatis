package net.butfly.albatis.io.pump;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.google.common.base.Supplier;
import com.hzcominfo.albatis.Albatis;

import net.butfly.albacore.base.Namedly;
import net.butfly.albacore.io.Openable;
import net.butfly.albacore.lambda.Runnable;
import net.butfly.albacore.paral.Exeter;
import net.butfly.albacore.utils.OpenableThread;

abstract class PumpImpl<V, P extends PumpImpl<V, P>> extends Namedly implements Pump<V> {
	protected static final int STATUS_OTHER = 0;
	protected static final int STATUS_RUNNING = 1;
	protected static final int STATUS_STOPPED = 2;

	protected final String name;
	private final int parallelism;
	protected int batchSize;

	private final List<OpenableThread> tasks = new ArrayList<>();
	protected final List<AutoCloseable> dependencies;

	protected PumpImpl(String name, int parallelism) {
		super(name);
		this.name = name;
		if (parallelism < 0) this.parallelism = (int) Math.floor(Math.sqrt(Exeter.of().parallelism())) - parallelism;
		else if (parallelism == 0) this.parallelism = 16;
		else this.parallelism = parallelism;
		dependencies = new ArrayList<>();
		batchSize = Integer.parseInt(System.getProperty(Albatis.Props.PROP_PUMP_BATCH_SIZE, "1000"));
		logger().info("Pump [" + name + "] created with parallelism: " + parallelism);
	}

	@Override
	public Pump<V> batch(int batchSize) {
		this.batchSize = batchSize;
		return this;
	}

	private void closeDeps() {
		for (AutoCloseable dep : dependencies)
			try {
				dep.close();
			} catch (Exception e) {
				logger().error(dep.getClass().getName() + " close failed");
			}
	}

	protected final void depend(List<? extends AutoCloseable> dependencies) {
		this.dependencies.addAll(dependencies);
	}

	protected final void depend(AutoCloseable... dependencies) {
		depend(Arrays.asList(dependencies));
	}

	protected final void pumping(Supplier<Boolean> sourceEmpty, Runnable pumping) {
		Runnable r = Runnable.exception(pumping::run, ex -> logger().error("Pump processing failure", ex)).until(() -> {
			return !opened() || sourceEmpty.get();
		});
		for (int i = 0; i < parallelism; i++)
			tasks.add(new OpenableThread(r, name() + "PumpThread#" + i));
	}

	@Override
	public void open() {
		Pump.super.open();
		for (OpenableThread t : tasks)
			t.open();
		try {
			for (OpenableThread t : tasks)
				try {
					t.join();
				} catch (InterruptedException e) {
					t.close();
				}
		} finally {
			close();
		}
		logger().info(name() + " finished.");
	}

	protected boolean isAllDependsOpen() {
		return dependencies.stream().map(c -> {
			if (c instanceof Openable) return ((Openable) c).opened();
			else return true;
		}).reduce((o1, o2) -> o1 && o2).orElse(true);
	}

	@Override
	public boolean opened() {
		return Pump.super.opened() && isAllDependsOpen();
	}

	@Override
	public void close() {
		Pump.super.close();
		closeDeps();
	}
}
