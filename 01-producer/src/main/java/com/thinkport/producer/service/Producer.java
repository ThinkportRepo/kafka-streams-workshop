package com.thinkport.producer.service;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.datafaker.Faker;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class Producer {
  public Producer(KafkaTemplate<String, String> template){
      this.template = template;
  }
  @Value("${spring.kafka.bootstrap-servers}")
  private String bootstrapServer;

  private static final String TOPIC = "my-first-topic";
  private final KafkaTemplate<String, String> template;
  private final Faker faker = new Faker();

  //@Scheduled(fixedRate = 10000)
  public void sendPlainJava() {

    // create Producer properties
    Properties properties = new Properties();

    KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

  }

  @Scheduled(fixedRate = 10000)
  public void sendSpringBoot() {
      //template.send("producerRecord").get(10, TimeUnit.SECONDS);
  }
}
