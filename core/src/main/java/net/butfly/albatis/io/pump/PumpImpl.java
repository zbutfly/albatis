package net.butfly.albatis.io.pump;

import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Supplier;

import net.butfly.albacore.base.Namedly;
import net.butfly.albacore.io.Openable;
import net.butfly.albacore.lambda.Runnable;
import net.butfly.albacore.paral.Exeter;
import net.butfly.albacore.utils.Exceptions;
import net.butfly.albacore.utils.OpenableThread;
import net.butfly.albacore.utils.collection.Colls;

public abstract class PumpImpl<V, P extends PumpImpl<V, P>> extends Namedly implements Pump<V> {
	protected static final int STATUS_OTHER = 0;
	protected static final int STATUS_RUNNING = 1;
	protected static final int STATUS_STOPPED = 2;

	protected final String name;
	private final int parallelism;

	private final List<OpenableThread> tasks = new ArrayList<>();
	protected final List<Openable> dependencies;

	protected PumpImpl(String name, int parallelism) {
		super(name);
		this.name = name;
		if (parallelism < 0) this.parallelism = (int) Math.floor(Math.sqrt(Exeter.of().parallelism())) - parallelism;
		else if (parallelism == 0) this.parallelism = 16;
		else this.parallelism = parallelism;
		dependencies = Colls.list();
		logger().info("Pump [" + name + "] created with parallelism: " + parallelism);
	}

	private void closeDeps() {
		for (AutoCloseable dep : dependencies)
			try {
				dep.close();
			} catch (Exception e) {
				logger().error(dep.getClass().getName() + " close failed", e);
			}
	}

	protected final void depend(List<Openable> dependencies) {
		this.dependencies.addAll(Colls.list(dependencies));
	}

	protected final void pumping(Supplier<Boolean> sourceEmpty, Runnable pumping) {
		Runnable r = Runnable.exception(pumping::run, //
				ex -> logger().error("Pump processing failure for [" + ex.toString() + "] at: \n" + Exceptions.getStackTrace(ex)))//
				.until(() -> !opened() || sourceEmpty.get());
		for (int i = 0; i < parallelism; i++)
			tasks.add(new OpenableThread(r, name() + "PumpThread#" + i));
	}

	@Override
	public void open() {
		Pump.super.open();
		for (int i = dependencies.size() - 1; i >= 0; i--)
			dependencies.get(i).open();
		for (OpenableThread t : tasks)
			t.open();
		for (OpenableThread t : tasks)
			try {
				t.join();
			} catch (InterruptedException e) {
				t.close();
			}
		logger().info("Pumping[" + name() + "] finished.");
	}

	protected boolean isAllDependsOpen() {
		for (AutoCloseable dep : dependencies)
			if (dep instanceof Openable && !((Openable) dep).opened()) return false;
		return true;
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
