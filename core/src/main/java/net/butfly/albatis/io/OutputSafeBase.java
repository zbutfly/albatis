package net.butfly.albatis.io;

import static net.butfly.albatis.io.IOProps.propI;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.concurrent.atomic.AtomicInteger;

import net.butfly.albacore.base.Namedly;
import net.butfly.albacore.io.lambda.Supplier;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.paral.Task;
import net.butfly.albacore.utils.Configs;
import net.butfly.albatis.Albatis;

/**
 * limit flow by prop <code>paral.limit</code>, and wait full finish on closing.
 */
abstract class OutputSafeBase<V> extends Namedly implements Output<V> {
	private static final long serialVersionUID = 7728355644701942166L;
	private static final String PARAL_LIMIT = "paral.limit";
	protected final int BATCH_SIZE = batchSize();
	protected final Supplier<Boolean> opExceeded;
	protected final AtomicInteger opsPending;

	protected OutputSafeBase(String name) {
		super(name);
		this.opsPending = new AtomicInteger(0);
		int maxOps = propI(getClass(), PARAL_LIMIT, 0);
		this.opExceeded = maxOps > 0 ? () -> opsPending.get() > maxOps : () -> false;
	}

	protected abstract void enqsafe(Sdream<V> items);

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

	@Deprecated
	protected int detectOldMax() {
		Object o = Wrapper.bases(this);
		Class<?> oc = o.getClass();
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
						cop = Integer.parseInt(Configs.gets(confn, Integer.toString(defv)));
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
