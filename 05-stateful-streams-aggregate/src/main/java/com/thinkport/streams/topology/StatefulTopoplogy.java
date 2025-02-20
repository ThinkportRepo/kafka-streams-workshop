package com.thinkport.streams.topology;

import com.thinkport.streams.config.CustomSerdes;
import digital.thinkport.avro.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Properties;

@Component
@Slf4j
public class StatefulTopoplogy {

  @Value("${spring.kafka.properties.schema.registry.url}")
  private String schemaRegistryUrl;

  @Value("${kafka-topics.shop-cart-in}")
  private String cartItemTopicIn;

  @Value("${kafka-topics.articles-in}")
  private String articlesTopicIn;

  @Value("${kafka-topics.shop-cart-out}")
  private String shopCartOut;

  @Bean
  public KStream<String, CartItem> cartItemStream(StreamsBuilder kStreamBuilder) {
    return kStreamBuilder.stream("S");
  }

  @Bean
  public GlobalKTable<String, Article> articles(StreamsBuilder kStreamBuilder) {
    return kStreamBuilder.globalTable("G");
  }

  @Bean
  public KStream<String, CartItemPrice> cartItemPriceStream(
      KStream<String, CartItem> cartItemStream, GlobalKTable<String, Article> articles) {
    return null;
  }

  private ValueJoinerWithKey<String, CartItem, Article, CartItemPrice> cartItemPriceValueJoiner() {
return null;
  }

  private Initializer<ShoppingCartAggregate> shoppingCartAggregateInitializer() {
    return null;
  }

  private Aggregator<String, CartItemPrice, ShoppingCartAggregate> shoppingCartAggregator() {
    return null;
  }

  @Bean
  public KTable<String, ShoppingCartAggregate> shoppingCartAggregateTable(
      KStream<String, CartItemPrice> cartItemPrice) {
    return null;
  }

  private Properties getSchemaProperties() {
    Properties properties = new Properties();
    properties.put("schema.registry.url", schemaRegistryUrl);
    return properties;
  }
}
