package net.butfly.albatis.bcp;

import net.butfly.albacore.expr.fel.FelFunc;
import net.butfly.albacore.expr.fel.FelFunc.Func;
import net.butfly.albatis.bcp.utils.HttpUtils;

import java.util.Base64;

public interface FuncBcp {
	@Func
	class UrlField2Base64Func extends FelFunc<String> {
		@Override
		protected boolean valid(int argl) {
			return argl == 1;
		}

		@Override
		protected String invoke(Object... args) {
			if (null == args[0] || !CharSequence.class.isAssignableFrom(args[0].getClass())) return null;
			String url = (String) args[0];
			byte[] data = HttpUtils.get(url);
			return null == data || data.length == 0 ? null : Base64.getEncoder().encodeToString(data);
		}
	}

}
