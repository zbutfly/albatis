/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.hzcominfo.tools.kafkaconsumer.api;

import com.hzcominfo.tools.kafkaconsumer.config.KafkaConsumerConfig;
import com.hzcominfo.tools.kafkaconsumer.context.DataContext;
import com.hzcominfo.tools.kafkaconsumer.thread.ConsumerThread;
import com.hzcominfo.tools.kafkaconsumer.context.ResultContext;
import com.hzcominfo.tools.kafkaconsumer.mapper.KafkaMessage;
import com.hzcominfo.tools.kafkaconsumer.mapper.KafkaTopicConfig;
import com.hzcominfo.tools.kafkaconsumer.thread.MessageTranportThread;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

/**
 *
 * @author hzcominfo
 */
public class KafkaConsumer {

    //consumer对象名字
    private String consumerName = "DEFAULT CONSUMER";
    //topic-keyfield
    Map<String, String> topicKeyFieldMap = new HashMap<>();

    //传输最大
    int tranMax;

    MessageTranportThread mtt;

    /**
     * get consumer name
     *
     * @return the consumer name
     */
    public String getConsumerName() {
        return consumerName;
    }

    /**
     * set consumer name
     *
     * @param consumerName
     */
    public void setConsumerName(String consumerName) {
        this.consumerName = consumerName;
    }
    //kafka连接对象
    //一个kafkaConsumer只启用一个
    ConsumerConnector kafkaConnector;
    //线程池
    ExecutorService kafkaConsumerService;

    //初始化状态
    boolean initFlag = false;

    /**
     * 初始化kafka consumer流程
     *
     * @param kafkaConfig kafka连接配置
     * @param topics topic及stream配置
     * @param bufferMix 数据缓冲是否混合存放ture:mix|false:not mix
     * @param msgBuffers 数据缓冲区个数（如果bufferMix为false则无效）
     * @param bufferMax 数据缓冲区最大容纳message数量
     * @return ResultContext
     */
    public String init(final KafkaConsumerConfig kafkaConfig, final KafkaTopicConfig[] topics,
            final boolean bufferMix, final int msgBuffers, final int bufferMax) {
        //已启动则不进行初始化
        if (initFlag) {
            return ResultContext.RESULT_INIT_ALREADY_RUN;
        }

        //初始化线程池
        int poolMax = countThreadPoolMax(topics);
        if (poolMax == 0) {
            return ResultContext.RESULT_INIT_NONE_TOPICS;
        }

        tranMax = bufferMax;

        kafkaConsumerService = Executors.newFixedThreadPool(poolMax);

        //初始化kafka
        //config
        if (kafkaConfig.zookeeper_connect == null || kafkaConfig.group_id == null) {
            return ResultContext.RESULT_INIT_CONFIG_NULL;
        }

        Properties props = new Properties();
        props.put("zookeeper.connect", kafkaConfig.zookeeper_connect);
        props.put("zookeeper.connection.timeout.ms", "" + kafkaConfig.zookeeper_connection_timeout_ms);
        props.put("zookeeper.sync.time.ms", "" + kafkaConfig.zookeeper_sync_time_ms);
        props.put("group.id", kafkaConfig.group_id);
        props.put("auto.commit.enable", "" + kafkaConfig.auto_commit_enable);
        props.put("auto.commit.interval.ms", "" + kafkaConfig.auto_commit_interval_ms);
        props.put("auto.offset.reset", kafkaConfig.auto_offset_reset);
        props.put("session.timeout.ms", "" + kafkaConfig.session_timeout_ms);
        props.put("partition.assignment.strategy", kafkaConfig.partition_assignment_strategy);
        props.put("socket.receive.buffer.bytes", "" + kafkaConfig.socket_receive_buffer_bytes);
        props.put("fetch.message.max.bytes", "" + kafkaConfig.fetch_message_max_bytes);
        ConsumerConfig consumerConfig = new ConsumerConfig(props);

        //init connector
        int threadCount = 0;
        try {

            kafkaConnector = Consumer.createJavaConsumerConnector(consumerConfig);
            Map<String, List<KafkaStream<byte[], byte[]>>> topicMessageStreams = kafkaConnector
                    .createMessageStreams(topicArrayToTopicMap(topics));

            for (Map.Entry<String, List<KafkaStream<byte[], byte[]>>> messageStreamsOfTopic : topicMessageStreams
                    .entrySet()) {
                List<KafkaStream<byte[], byte[]>> streams = messageStreamsOfTopic.getValue();
                if (streams.isEmpty()) {
                    continue;
                }

                for (final KafkaStream<byte[], byte[]> stream : streams) {
                    ConsumerThread ct = new ConsumerThread();
                    ct.setStream(stream);
                    ct.setTopicKeyFieldMap(topicKeyFieldMap);
                    ct.setMsgListMax(bufferMax * msgBuffers);
                    kafkaConsumerService.submit(ct);
                    threadCount++;
                }
            }

        } catch (Exception e) {
            return ResultContext.RESULT_INIT_CONN_ERROR;
        }
        System.out.println("[" + consumerName + "]START CONSUMER THREAD:" + threadCount);

        //初始化传输线程
        mtt = new MessageTranportThread();
        mtt.setBuffMax(bufferMax);
        mtt.setBuffNum(msgBuffers);
        mtt.setMixFlag(bufferMix);
        mtt.setTopics(topics);
        mtt.setName("Transport_Thread");

        System.out.println("[" + consumerName + "]START TRANSPORT THREAD");
        mtt.start();

        initFlag = true;
        return ResultContext.RESULT_SUCCESS;
    }

    /**
     * get kafkaConsumer's running status
     *
     * @return true:running|false:not run or in error
     */
    public boolean getConsumerStatus() {
        return initFlag;
    }

    /**
     * commit to zookeeper
     */
    public void commit() {
        kafkaConnector.commitOffsets();
    }

    /**
     * 关闭kafka consumer相关线程，关闭kafka连接，清空缓存数据
     *
     * @return SUCCESS
     */
    public String uninit() {
        try {
            System.out.println("[" + consumerName + "]CLOSE ALL THREADS");
            kafkaConsumerService.shutdown();
            kafkaConnector.shutdown();
            mtt.stopFlag = true;

            System.out.println("[" + consumerName + "]CLEAR BUFFERS");
            DataContext.msgList.clear();
            DataContext.msgMap.clear();
            DataContext.msgPcgList.clear();
        } catch (Exception e) {
        }

        initFlag = false;
        return ResultContext.RESULT_SUCCESS;
    }

    /**
     * get message list in the queue
     *
     * @param topic mix:""|not mix: topic name
     * @return no data:null|have data:List<KafkaMessage>
     */
    public synchronized List<KafkaMessage> getMessagePackage(String topic) {
        List<KafkaMessage> sendList = new ArrayList<>();
        if ("".equals(topic)) {
            if (DataContext.msgPcgList.size() > 1) {
                sendList.addAll(DataContext.msgPcgList.remove(0));
            } else {
                return null;
            }
        } else {
            if (DataContext.msgMap.containsKey(topic)) {
                List<KafkaMessage> msgList = DataContext.msgMap.get(topic);
                while (!msgList.isEmpty() && sendList.size() < tranMax) {
                    sendList.add(msgList.remove(0));
                }
            } else {
                return null;
            }
        }
        return sendList;
    }

    /**
     * 获取线程池max值
     *
     * @param topics
     * @return int for all streams count of all topic
     */
    private int countThreadPoolMax(KafkaTopicConfig[] topics) {
        int count = 0;
        for (KafkaTopicConfig topic : topics) {
            count += topic.getStreamNum();
        }
        return count;
    }

    /**
     * get topic map
     *
     * @param topics
     * @return Map<String,Integer>,key is topic name,value is stream count
     */
    private Map<String, Integer> topicArrayToTopicMap(KafkaTopicConfig[] topics) {
        Map<String, Integer> topicMap = new HashMap<>();
        for (KafkaTopicConfig topic : topics) {
            topicMap.put(topic.getTopic(), topic.getStreamNum());
            topicKeyFieldMap.put(topic.getTopic(), topic.getKey());
        }

        return topicMap;
    }

}
