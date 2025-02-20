package com.thinkport.streams.topology;

import com.thinkport.producer.model.ClickJson;
import com.thinkport.streams.config.CustomSerdes;
import digital.thinkport.avro.ClickAvro;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
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
  private String schemaRegistryUrl;
  private String clicksTopicIn;
  private String clicksFilteredTopicOut;
  private String clicksErroneousTopicOut;

  public StatelessTopoplogy(
          @Value("${spring.kafka.properties.schema.registry.url}") String schemaRegistryUrl,
          @Value("${kafka-topics.clicks-in}")String clicksTopicIn,
          @Value("${kafka-topics.clicks-filtered-out}") String clicksFilteredTopicOut,
          @Value("${kafka-topics.clicks-erroneous-out}")String clicksErroneousTopicOut) {
    this.clicksTopicIn = clicksTopicIn;
    this.schemaRegistryUrl = schemaRegistryUrl;
    this.clicksFilteredTopicOut = clicksFilteredTopicOut;
    this.clicksErroneousTopicOut = clicksErroneousTopicOut;
  }

  @Bean
  public KStream<String, ClickJson> statelessStream(StreamsBuilder kStreamBuilder) {
    KStream<String, ClickJson> stream =
        kStreamBuilder.stream(
            clicksTopicIn, Consumed.with(Serdes.String(), new JsonSerde<>(ClickJson.class)));
    KStream<String, ClickAvro> transformedStream =
        stream
            .peek((k, v) -> LOG.info("Key: " + k + ", Value: " + v))
            .mapValues(
                clickJson -> {
                  return ClickAvro.newBuilder()
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
                });
    Map<String, KStream<String, ClickAvro>> branches =
        transformedStream
            .split(Named.as("Branch-"))
            .branch(
                (key, value) -> value.getUserId().equals(ADMIN_USER_ID), /* first predicate  */
                Branched.as("Erroneous"))
            .defaultBranch(Branched.as("Filtered"));
    branches
        .get("Branch-Erroneous")
        .to(
            clicksErroneousTopicOut,
            Produced.with(Serdes.String(), CustomSerdes.getClickSerde(getSchemaProperties())));
    branches
        .get("Branch-Filtered")
        .to(
            clicksFilteredTopicOut,
            Produced.with(Serdes.String(), CustomSerdes.getClickSerde(getSchemaProperties())));
    return stream;
  }

  private Properties getSchemaProperties() {
    Properties properties = new Properties();
    properties.put("schema.registry.url", schemaRegistryUrl);
    return properties;
  }
}
