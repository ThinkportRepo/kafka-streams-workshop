package com.thinkport.producer.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

@Configuration
public class Topics {
    @Bean
    public KafkaAdmin.NewTopics workshopTopics() {
        return new KafkaAdmin.NewTopics(
                TopicBuilder.name("my-first-topic")
                        .replicas(1)
                        .partitions(3)
                        .compact()
                        .build());
    }
}
