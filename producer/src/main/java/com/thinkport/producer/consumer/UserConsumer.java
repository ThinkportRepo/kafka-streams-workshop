package com.thinkport.producer.consumer;

import com.thinkport.producer.model.User;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

@Service
public class UserConsumer {
/*
    //@KafkaListener(topics = "user.topic", containerFactory = "userKafkaListenerFactory")
    public void consumeJson(User user) {
        System.out.println("Consumed JSON Message: " + user.toString());
        //ack.acknowledge();
    }

 */
}
