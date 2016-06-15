/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.hzcominfo.tools.kafkaconsumer.mapper;

/**
 *
 * @author hzcominfo
 */
public class KafkaTopicConfig {

    //topic name
    private String topic;
    //stream count for this topic
    private Integer streamNum;
    //key field
    private String key;

    /**
     * get key field
     *
     * @return key
     */
    public String getKey() {
        if (key == null) {
            return "";
        }
        return key;
    }

    /**
     * set key field
     *
     * @param key key field
     */
    public void setKey(String key) {
        this.key = key;
    }

    /**
     * get topic name
     *
     * @return topic name|""
     */
    public String getTopic() {
        if (topic == null) {
            return null;
        }
        return topic;
    }

    /**
     * set topic name
     *
     * @param topic
     */
    public void setTopic(String topic) {
        this.topic = topic;
    }

    /**
     * get topic stream count
     *
     * @return count
     */
    public Integer getStreamNum() {
        if (streamNum == null) {
            return 0;
        }
        return streamNum;
    }

    /**
     * set stream count for this topic
     *
     * @param streamNum
     */
    public void setStreamNum(Integer streamNum) {
        this.streamNum = streamNum;
    }

}
