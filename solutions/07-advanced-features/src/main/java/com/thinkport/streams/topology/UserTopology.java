package com.thinkport.streams.topology;

import com.thinkport.streams.config.CustomSerdes;
import digital.thinkport.avro.User;
import java.util.Map;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class UserTopology {
  private String schemaRegistryUrl;

  public UserTopology(
          @Value("${spring.kafka.properties.schema.registry.url}") String schemaRegistryUrl){
    this.schemaRegistryUrl = schemaRegistryUrl;
  }
  @Bean
  public KTable<String, User> userTable(StreamsBuilder kStreamBuilder) {
    Map<String,String> topicConfig = Map.of(
            "cleanup.policy", "delete,compact",
            "retention.ms","86400000"
    );
      Materialized<String,User, KeyValueStore<Bytes, byte[]>> userStateStore =
              Materialized.<String,User>as(Stores.persistentKeyValueStore("all-users"))
                      .withKeySerde(Serdes.String())
                      .withValueSerde(CustomSerdes.getUserSerde(getSchemaProperties()))
                      .withCachingDisabled()
                      .withLoggingEnabled(topicConfig);
    return kStreamBuilder.table ("shop.users",Consumed.with(Serdes.String(),CustomSerdes.getUserSerde(getSchemaProperties())),userStateStore);
  }

  private Properties getSchemaProperties() {
    Properties properties = new Properties();
    properties.put("schema.registry.url", schemaRegistryUrl);
    return properties;
  }
}
