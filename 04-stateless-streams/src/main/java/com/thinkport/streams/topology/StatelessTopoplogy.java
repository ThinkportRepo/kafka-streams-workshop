package com.thinkport.streams.topology;

import com.thinkport.producer.model.ClickJson;
import com.thinkport.streams.config.CustomSerdes;
import digital.thinkport.avro.ClickAvro;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Properties;

@Component
@Slf4j
public class StatelessTopoplogy {
  private final String ADMIN_USER_ID = "9000";

  @Value("${spring.kafka.properties.schema.registry.url}")
  private String schemaRegistryUrl;

  @Value("${kafka-topics.clicks-in}")
  private String clicksTopicIn;

  @Value("${kafka-topics.clicks-out}")
  private String clicksTopicOut;

  @Value("${kafka-topics.clicks-filtered-out}")
  private String clicksFilteredTopicOut;

  @Value("${kafka-topics.clicks-erroneous-out}")
  private String clicksErroneousTopicOut;

  @Autowired
  public void statelessStream(StreamsBuilder kStreamBuilder) {
  }

  private Properties getSchemaProperties() {
    Properties properties = new Properties();
    properties.put("schema.registry.url", schemaRegistryUrl);
    return properties;
  }
}
