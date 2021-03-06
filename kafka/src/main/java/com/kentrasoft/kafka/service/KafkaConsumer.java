package com.kentrasoft.kafka.service;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class KafkaConsumer {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumer.class);

    /**
     * 监听消息
     * 主题：upload
     * @param message
     */
    @KafkaListener(topics = {"${spring.kafka.topic.nb-upload}"}, groupId = "0")
    public void consumerUpload(String message){
        log.info("接收upload主题消息 : " + message);
    }

    /**
     * 监听消息
     * 主题：callback
     * @param message
     */
    @KafkaListener(topics = {"${spring.kafka.topic.nb-callback}"}, groupId = "0")
    public void consumerCallback(String message){
        log.info("接收callback主题消息 : " + message);
    }

    /**
     * 监听消息
     * 主题：handle
     * @param message
     */
    @KafkaListener(topics = {"${spring.kafka.topic.nb-handle}"}, groupId = "0")
    public void consumerHandle(String message){
        log.info("接收handle主题消息 : " + message);
    }

    /**
     * 监听消息
     * 主题：subscribe
     * @param message
     */
    @KafkaListener(topics = {"${spring.kafka.topic.nb-subscribe}"}, groupId = "0")
    public void consumerSubscribe(String message){
        log.info("接收subscribe主题消息 : " + message);
    }

    /**
     * 监听消息
     * 主题：subscribe
     * @param message
     */
    @KafkaListener(topics = {"${spring.kafka.topic.nb-agps}"}, groupId = "0")
    public void consumerAgps(String message){
        log.info("接收agps主题消息 : " + message);
    }

    /**
     * 监听消息
     * 主题：cd-upload
     * @param message
     */
    @KafkaListener(topics = {"${spring.kafka.topic.nb-cd-upload}"}, groupId = "0")
    public void consumerCdUpload(String message){
        log.info("接收cd-upload主题消息 : " + message);
    }
}
