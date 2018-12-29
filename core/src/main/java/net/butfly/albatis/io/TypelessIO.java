package net.butfly.albatis.io;

import java.util.List;
import java.util.Map;

import net.butfly.albacore.utils.Configs;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.alserder.SerDes;

@SuppressWarnings("rawtypes")
public interface TypelessIO extends IOFactory {
	static String DEFAULT_FORMAT = Configs.gets("albatis.format.default", "bson");

	@Override
	default List<SerDes> serdes() {
		return _Priv.SDS.computeIfAbsent(this, c -> _Priv.calcSerdes(IOFactory.super.serdes()));
	}

	class _Priv {
		private static final Map<TypelessIO, List<SerDes>> SDS = Maps.of();

		private static List<SerDes> calcSerdes(List<SerDes> sds) {
			SerDes sd = null == sds || sds.isEmpty() ? null : sds.get(0);
			@SuppressWarnings("unchecked")
			SerDes def = SerDes.sd(DEFAULT_FORMAT);
			if (null == sd) return Colls.list(def);
			if (null == def || def.format().equals(sd.format())) return sds;
			Class toc = sd.toClass();
			if (null == toc || (!String.class.isAssignableFrom(toc) && !byte[].class.isAssignableFrom(toc))) sds.add(0, def);// need ser
			return sds;
		}
	}

	default SerDes nativeSD() {
		List<SerDes> sds = serdes();
		return null == sds || sds.isEmpty() ? null : sds.get(0);
	}
}
