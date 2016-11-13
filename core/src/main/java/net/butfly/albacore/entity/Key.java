package net.butfly.albacore.entity;

import net.butfly.albacore.support.Bean;
import net.butfly.albacore.support.Beans;
import net.butfly.albacore.utils.Objects;

public abstract class Key<K extends Key<K>> extends Bean<AbstractEntity<K>> implements AbstractEntity<K> {
	private static final long serialVersionUID = 1L;

	@SuppressWarnings("unchecked")
	@Override
	public K getId() {
		return (K) this;
	}

	@Override
	public void setId(K id) {
		Objects.copy(id, this);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compareTo(Beans<AbstractEntity<K>> o) {
		if (null != o && o instanceof Entity) return Objects.compare(this, ((Entity) o).id);
		else return -1;
	}
}
