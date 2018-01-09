package net.butfly.albatis.io;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import com.hzcominfo.albatis.Albatis;

import net.butfly.albacore.base.Namedly;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.paral.Task;
import net.butfly.albacore.utils.Configs;

abstract class SafeOutputBase<V> extends Namedly implements Output<V> {
	private static final String PARAL_LIMIT = "paral.limit";
	protected final Supplier<Boolean> opExceeded;
	protected final AtomicInteger opsPending;

	protected SafeOutputBase(String name) {
		super(name);
		this.opsPending = new AtomicInteger(0);
		int maxOps = detectMaxConcurrentOps();
		this.opExceeded = maxOps > 0 ? () -> opsPending.get() > maxOps : () -> false;
	}

	protected abstract void enqSafe(Sdream<V> items);

	@Override
	public void close() {
		logger().info("INFO: " + name() + " closing after safe waiting @[" + Thread.currentThread().toString() + "], "//
				+ "pending ops:" + opsPending.get());
		int remained;
		int waited = 1;
		while (0 < (remained = opsPending.get())) {
			int r = remained;
			logger().debug("Output ops [" + r + "] remained, waiting " + (waited++) + " second for safe closing.");
			if (!Task.waitSleep(1000)) break;
		}
		Output.super.close();
	}

	@Override
	public String toString() {
		return super.toString() + "[Pending Ops: " + opsPending.get() + "]";
	}

	private int detectMaxConcurrentOps() {
		return Props.propI(getClass(), PARAL_LIMIT, 0);
	}

	@Deprecated
	protected int detectOldMax() {
		Class<?> oc = (this instanceof Wrapper ? (Output<?>) ((Wrapper<?>) this).bases() : this).getClass();
		int cop = -1;
		Field f;
		int mod;
		try {
			if (null != (f = oc.getField(Albatis.MAX_CONCURRENT_OP_FIELD_NAME))) {
				mod = f.getModifiers();
				if (Modifier.isStatic(mod) && Modifier.isPublic(mod) && Modifier.isFinal(mod) //
						&& CharSequence.class.isAssignableFrom(f.getType())) {
					CharSequence conf;
					if (null != (conf = (CharSequence) f.get(null))) {
						int defv = -1;
						if (null != (f = oc.getField(Albatis.MAX_CONCURRENT_OP_FIELD_NAME_DEFAULT))) {
							mod = f.getModifiers();
							if (Modifier.isStatic(mod) && Modifier.isPublic(mod) && Modifier.isFinal(mod)) defv = f.getInt(null);
						}
						String confn = conf.toString();
						String info = "Output [" + oc.toString() + "] concurrent limit configurated by -D" + confn;
						cop = Integer.parseInt(Configs.of().gets(confn, Integer.toString(defv)));
						if (cop > 0) info += "=" + cop;
						if (defv > 0) info += " [default:" + defv + "]";
						logger().info(info + ".");
					}
				}
			}
		} catch (Exception e) {}
		return cop;
	}
}
