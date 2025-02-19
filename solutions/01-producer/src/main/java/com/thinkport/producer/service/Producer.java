package com.thinkport.producer.service;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import net.datafaker.Faker;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringSerializer;
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

  @Scheduled(fixedRate = 10000)
  public void sendPlainJava() {
    String bootstrapServers = bootstrapServer;

    // create Producer properties
    Properties properties = new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

    ArrayList<Header> headers = new ArrayList<>();
    headers.add(new RecordHeader("traceId", faker.idNumber().valid().getBytes(StandardCharsets.UTF_8)));

    ProducerRecord<String, String> producerRecord =
            new ProducerRecord<>(TOPIC, null,"key",faker.elderScrolls().city(),headers);
    producer.send(producerRecord, (recordMetadata, e) -> {
      // executes every time a record is successfully sent or an exception is thrown
      if (e == null) {
        // the record was successfully sent
        log.info("Received new metadata. \n" +
                "Topic:" + recordMetadata.topic() + "\n" +
                "Partition: " + recordMetadata.partition() + "\n" +
                "Offset: " + recordMetadata.offset() + "\n" +
                "Timestamp: " + recordMetadata.timestamp());
      } else {
        log.error("Error while producing", e);
      }
    });
  }

  @Scheduled(fixedRate = 10000)
  public void sendSpringBoot() {
    ArrayList<Header> headers = new ArrayList<>();
    headers.add(new RecordHeader("traceId", faker.idNumber().valid().getBytes(StandardCharsets.UTF_8)));

    ProducerRecord<String, String> producerRecord =
            new ProducerRecord<>(TOPIC, null,"key",faker.elderScrolls().city(),headers);
    try {
      template.send(producerRecord).get(10, TimeUnit.SECONDS);
      log.info("Sent record with Spring Boot template.");
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      log.info("Failed to wait for record send.", e);
    } catch (Exception e) {
      log.info("Failed to send record.", e);
    }

  }
}
