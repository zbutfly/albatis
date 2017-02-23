//package net.butfly.albatis.kafka;
//
//import java.io.IOException;
//
//import kafka.consumer.KafkaStream;
//import net.butfly.albacore.exception.ConfigException;
//import net.butfly.albacore.io.InputImpl;
//import net.butfly.albacore.io.URISpec;
//import net.butfly.albacore.lambda.Consumer;
//
//abstract class KafkaInputBase<V> extends InputImpl<KafkaMessage> {
//
//	public KafkaInputBase(String name, URISpec kafkaURI) throws ConfigException {}
//
//	public KafkaInputBase(String name, final String kafkaURI, String... topics) throws ConfigException, IOException {}
//
//	protected abstract V fetcher(KafkaStream<byte[], byte[]> s);
//
//	protected abstract KafkaMessage fetch(KafkaStream<byte[], byte[]> stream, V lock, Consumer<KafkaMessage> result);
//
//}
