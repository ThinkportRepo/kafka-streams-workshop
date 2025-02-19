package com.thinkport.producer.config;

import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Service;

@Service
public class Topics {
  @Bean
  public KafkaAdmin.NewTopics workshopTopics() {
    return new KafkaAdmin.NewTopics(
        TopicBuilder.name("shop.articles").replicas(1).partitions(3).compact().build(),
        TopicBuilder.name("shop.carts").replicas(1).partitions(3).build(),
        TopicBuilder.name("shop.clicks.raw").replicas(1).partitions(3).build(),
        TopicBuilder.name("shop.clicks.filtered").replicas(1).partitions(3).build(),
            TopicBuilder.name("shop.clicks.avro").replicas(1).partitions(3).build(),
            TopicBuilder.name("shop.orders.placed").replicas(1).partitions(3).build(),
            TopicBuilder.name("shop.carts").replicas(1).partitions(3).build(),
            TopicBuilder.name("shop.carts.aggregate").replicas(1).partitions(3).build());

  }
}
