/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.hzcominfo.tools.kafkaconsumer.mapper;

import java.util.Map;

/**
 *
 * @author hzcominfo
 */
public class KafkaMessage {

    //topic name
    private String topic;
    //value map
    private Map<String, Object> values;
    //value of key field
    private Object keyValue;

    /**
     * get topic name of the message
     *
     * @return topic name
     */
    public String getTopic() {
        return topic;
    }

    /**
     * set topic name of message
     *
     * @param topic
     */
    public void setTopic(String topic) {
        this.topic = topic;
    }

    /**
     * get value map
     *
     * @return Map<String,Object> key is field name ,value is value of field
     */
    public Map<String, Object> getValues() {
        return values;
    }

    /**
     * set value map
     *
     * @param values
     */
    public void setValues(Map<String, Object> values) {
        this.values = values;
    }

    /**
     * get the value of the key field
     *
     * @return Object|null:""
     */
    public Object getKeyValue() {
        if (keyValue == null) {
            return "";
        }
        return keyValue;
    }

    /**
     * set value of the key field
     *
     * @param keyValue the value of the key field
     */
    public void setKeyValue(Object keyValue) {
        this.keyValue = keyValue;
    }

}
