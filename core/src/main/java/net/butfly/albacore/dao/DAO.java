package net.butfly.albacore.dao;

import net.butfly.albacore.base.BizUnit;

public interface DAO extends BizUnit {
	default String namespace() {
		return (this.getClass().getName().replaceAll("(?i)dao", "").replaceAll("(?i)impl", ".") + ".").replaceAll("\\.+", ".");
	}
}
