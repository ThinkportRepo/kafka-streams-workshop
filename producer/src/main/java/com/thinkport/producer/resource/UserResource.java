package com.thinkport.producer.resource;

import com.thinkport.producer.model.User;
import lombok.AllArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service
@AllArgsConstructor
public class UserResource {

    private final KafkaTemplate<String, User> template;

    private static final String TOPIC = "user.topic";

    //@Scheduled(fixedRate = 1000)
    public void send(){
        template.send(TOPIC, new User("test","test", "12"));
        System.out.println("Published");
    }
}
