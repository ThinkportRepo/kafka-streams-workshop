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
public class StatelessTopoplogy {
  private final int ADMIN_CLICK_ID = 9000;
  private final int LAST_VALID_HTTP_RESPONSE = 201;

  @Value("${spring.kafka.properties.schema.registry.url}")
  private String schemaRegistryUrl;

  @Value("${kafka-topics.clicks-in}")
  private String clicksTopicIn;

  @Value("${kafka-topics.clicks-out}")
  private String clicksTopicOut;

  @Value("${kafka-topics.users-in}")
  private String users;

  @Bean
  public KStream<String, ClickAvro> clickStream(StreamsBuilder kStreamBuilder) {
    return kStreamBuilder.stream(
        clicksTopicIn,
        Consumed.with(Serdes.String(), CustomSerdes.getClickSerde(getSchemaProperties())));
  }

  @Bean
  public KTable<Windowed<String>, Long> anomalUserClicks(KStream<String, ClickAvro> stream) {
    return stream
        .map((k, v) -> new KeyValue<>(v.getUserId(), v.getUserId()))
        .groupByKey()
        .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(Duration.ofSeconds(30)))
        .count()
        .filter((userID, count) -> count >= 50);
  }

  @Bean
  KStream<String, UserClicks> userClicksKStream(KTable<Windowed<String>, Long> anomalUsers) {
    return anomalUsers
        .toStream()
        .map(
            (k, v) ->
                new KeyValue<>(
                    k.key(),
                    UserClicks.newBuilder().setUserID(k.key()).setClickCount(v).build()));
        //.peek((k, v) -> System.out.println("K: " + k + " v: " + v.toString()));

  }

  @Bean
  GlobalKTable<String, User> userGlobalTable(StreamsBuilder kStreamBuilder) {
    return kStreamBuilder.globalTable(
        users, Consumed.with(Serdes.String(), CustomSerdes.getUserSerde(getSchemaProperties())));
  }

  @Bean
  KStream<String, UserFraud> joinedStream (KStream<String, UserClicks> userClicksKStream,GlobalKTable<String, User> userGlobalTable ){
    return userClicksKStream.join(
                  userGlobalTable,
                  (userID, userClicks) -> userID,
                  (userClicks, user) -> UserFraud.newBuilder()
                          .setID(userClicks.getUserID())
                          .setName(user.getName())
                          .setMail(user.getMail())
                          .setAddress(user.getAddress())
                          .setPhone(user.getPhone())
                          .setClickCount(userClicks.getClickCount())
                          .build()
          );
            //.peek((k, v) -> System.out.println("K: " + k + " v: " + v.toString()));

}


  private Properties getSchemaProperties() {
    Properties properties = new Properties();
    properties.put("schema.registry.url", schemaRegistryUrl);
    return properties;
  }
}
