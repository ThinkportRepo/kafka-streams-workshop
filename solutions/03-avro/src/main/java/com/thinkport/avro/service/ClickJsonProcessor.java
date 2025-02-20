  package com.thinkport.avro.service;


import com.thinkport.producer.model.ClickJson;
import digital.thinkport.avro.ClickAvro;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@AllArgsConstructor
public class ClickJsonProcessor {
  private final KafkaTemplate<String, ClickAvro> template;
  @KafkaListener(topics = "${kafka-topics.clicks-in}")
  public void listen(ConsumerRecord<String, ClickJson> message) {
    ClickJson clickJson = message.value();
    log.info("Received message with key: {}", message.key());
    log.debug("Received message: {}", message);
    ClickAvro clickAvro = ClickAvro.newBuilder()
            .setClickId(clickJson.getClickId())
            .setBytes(clickJson.getBytes())
            .setIp(clickJson.getIp())
            .setKnownIp(clickJson.isKnownIp())
            .setUserId(clickJson.getUserId())
            .setProductId(clickJson.getProductId())
            .setReferrer(clickJson.getReferrer())
            .setStatus(clickJson.getStatus())
            .setRequest(clickJson.getRequest())
            .setUserAgent(clickJson.getUserAgent())
            .build();
    ProducerRecord<String, ClickAvro> producerRecord = new ProducerRecord<>("shop.clicks.avro",message.key(),clickAvro);
    template.send(producerRecord);

  }
}
