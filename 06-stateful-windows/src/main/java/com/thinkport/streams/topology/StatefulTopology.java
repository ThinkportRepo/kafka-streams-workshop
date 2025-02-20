package com.thinkport.streams.topology;

import com.thinkport.streams.config.CustomSerdes;
import digital.thinkport.avro.ClickAvro;
import digital.thinkport.avro.User;
import digital.thinkport.avro.UserClicks;
import digital.thinkport.avro.UserFraud;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Properties;

@Component
@Slf4j
public class StatefulTopology {
  @Value("${spring.kafka.properties.schema.registry.url}")
  private String schemaRegistryUrl;

  @Value("${kafka-topics.clicks-in}")
  private String clicksTopicIn;

  @Value("${kafka-topics.clicks-out}")
  private String clicksTopicOut;

  @Value("${kafka-topics.users-in}")
  private String users;

  @Value("${kafka-topics.users-out}")
  private String userClicksFraudsTopic;


  @Bean
  public KStream<String, ClickAvro> clickStream(StreamsBuilder kStreamBuilder) {
    return kStreamBuilder.stream("X");
  }

  @Bean
  GlobalKTable<String, User> userGlobalTable(StreamsBuilder kStreamBuilder) {
    return kStreamBuilder.globalTable("Y");
  }

  @Bean
  public KTable<Windowed<String>, Long> anomalUserClicks(KStream<String, ClickAvro> stream) {
    return null;
  }

  @Bean
  KStream<String, UserClicks> userClicksKStream(KTable<Windowed<String>, Long> anomalUsers) {
    return null;
  }


/*
TODO: comment in if progressed in exercise

  @Bean
  KStream<String, UserFraud> joinedStream (KStream<String, UserClicks> userClicksKStream,GlobalKTable<String, User> userGlobalTable ){
    return null;
  }

 */


  private Properties getSchemaProperties() {
    Properties properties = new Properties();
    properties.put("schema.registry.url", schemaRegistryUrl);
    return properties;
  }
}
