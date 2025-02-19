package com.thinkport.consumer.service;


import com.thinkport.producer.model.ClickJson;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class Consumer {
  @KafkaListener(topics = "${kafka-topics.clicks-in}")
  public void listen(ConsumerRecord<String, ClickJson> message) {
    log.info("Received message with key: {}", message.key());
    log.debug("Received message: {}", message);
  }
}
