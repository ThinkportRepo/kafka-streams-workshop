  package com.thinkport.avro.service;


import com.thinkport.producer.model.ClickJson;
import digital.thinkport.avro.ClickAvro;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class AvroConsumer {
  @KafkaListener(topics = "${kafka-topics.clicks-out}", containerFactory = "clickAvroConcurrentKafkaListenerContainerFactory")
  public void listen(ConsumerRecord<String, ClickAvro> message) {
    log.info("Received AVRO message with key: {}", message.key());
    log.debug("Received AVRO message: {}", message);
  }
}
