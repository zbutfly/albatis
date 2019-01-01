package net.butfly.albatis.io;

import static net.butfly.albatis.io.format.Format.CONST_FORMAT;
import static net.butfly.albatis.io.format.Format.as;
import static net.butfly.albatis.io.format.Format.of;

import java.util.List;
import java.util.Map;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.Annotations;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.logger.Loggable;
import net.butfly.albatis.io.format.Format;
import net.butfly.alserder.SerDes;

public interface Formatable extends Loggable {
	URISpec uri();

	static final Map<Formatable, List<Format>> FORMAT_INSTANCES = Maps.of();

	default List<Format> formats() {
		return FORMAT_INSTANCES.computeIfAbsent(this, c -> {
			String format = uri().fetchParameter("df");
			List<Format> fmts = null == format ? Colls.list() : Colls.list(f -> of(f), format.split(","));

			SerDes.As[] defs = as(getClass());
			if (defs.length > 1) logger().warn("Multiple default serdes as annotations marked on " + getClass().getName() + ", first ["
					+ Annotations.toString(defs[0]) + "] will be used. All serdes as: \n\t" + String.join("\n\t", Colls.list(
							Annotations::toString, defs)));
			Format def = defs.length == 1 ? of(defs[0].value()) : null;
			if (def.equals(CONST_FORMAT)) def = null;
			Format fmt1 = null == fmts || fmts.isEmpty() ? null : fmts.get(0);
			if (null == fmt1) {
				if (null == def) return Colls.list();
				logger().info("Non-format defined, default format [" + def.as().value() + "] used.");
				return Colls.list(def);
			}
			if (defs.length > 0) logger().info("Default format [" + def.as().value() + "] is ignored by [" //
					+ String.join(",", Colls.list(fmts, f -> f.as().value())) + "]");
			return fmts;
		});
	}
}
