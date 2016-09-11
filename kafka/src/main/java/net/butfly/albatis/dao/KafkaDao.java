package net.butfly.albatis.dao;

import java.io.Serializable;

import net.butfly.albacore.dao.SimpleDAO;
import net.butfly.albacore.entity.AbstractEntity;
import net.butfly.albacore.exception.NotImplementedException;
import net.butfly.albatis.impl.kafka.config.KafkaConsumerConfig;

@SuppressWarnings("unchecked")
public interface KafkaDao extends SimpleDAO {
	default void setConsumer(KafkaConsumerConfig consumer) {
		throw new NotImplementedException();
	}

	void init();

	default <K extends Serializable, E extends AbstractEntity<K>> K insert(final E entity) {
		throw new NotImplementedException();
	}

	default <K extends Serializable, E extends AbstractEntity<K>> K[] insert(final E... entity) {
		throw new NotImplementedException();
	}

	@Override
	<K extends Serializable, E extends AbstractEntity<K>> E[] select(Class<E> entityClass, K... topic);
}
