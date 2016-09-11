package net.butfly.albacore.dao;

import java.io.Serializable;

import net.butfly.albacore.entity.AbstractEntity;

@SuppressWarnings("unchecked")
public interface SimpleDAO extends DAO {
	<K extends Serializable, E extends AbstractEntity<K>> K insert(final E entity);

	<K extends Serializable, E extends AbstractEntity<K>> K[] insert(final E... entity);

	<K extends Serializable, E extends AbstractEntity<K>> E[] select(final Class<E> entityClass, final K... key);

}
