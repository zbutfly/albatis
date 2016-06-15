/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.hzcominfo.tools.kafkaconsumer.context;

import com.hzcominfo.tools.kafkaconsumer.mapper.KafkaMessage;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * @author hzcominfo
 */
public class DataContext {

    //kafka consumer的基础输出
    public static List<KafkaMessage> msgList = Collections.synchronizedList(new ArrayList<KafkaMessage>());

    public static Map<String, List<KafkaMessage>> msgMap = Collections.synchronizedMap(new HashMap<String, List<KafkaMessage>>());

    public static List<List<KafkaMessage>> msgPcgList = Collections.synchronizedList(new ArrayList<List<KafkaMessage>>());

}
