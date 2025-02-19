package com.thinkport.producer.consumer;

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
