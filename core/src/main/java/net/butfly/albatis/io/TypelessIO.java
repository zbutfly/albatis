package net.butfly.albatis.io;

import java.util.List;
import java.util.Map;

import net.butfly.albacore.utils.Configs;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.alserder.SD;

@SuppressWarnings("rawtypes")
public interface TypelessIO extends IOFactory {
	class _Native {
		private static final Map<TypelessIO, SD> REGS = Maps.of();
	}

	default boolean unmatch(Class<?> toc) {
		return !String.class.isAssignableFrom(toc) && !byte[].class.isAssignableFrom(toc);
	}

	@Override
	default List<SD> serders(String... formats) {
		List<SD> sds = IOFactory.super.serders(formats);
		String df = Configs.gets("albatis.format.default", "bson");
		SD first = sds.isEmpty() ? SD.lookup(df) : sds.get(0);
		if (unmatch(first.toClass())) {
			if (Input.class.isAssignableFrom(this.getClass())) // need ser
				sds.add(0, first = SD.lookup(df));
			else if (Output.class.isAssignableFrom(this.getClass())) // need der
				sds.add(first = SD.lookup(df));
			else throw new IllegalArgumentException();
		}
		_Native.REGS.put(this, first);
		return sds;
	}

	default SD nativeSerder() {
		return _Native.REGS.get(this);
	}
}
