package com.thinkport.streams.topology;

import com.thinkport.producer.model.ClickJson;
import com.thinkport.streams.config.CustomSerdes;
import digital.thinkport.avro.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.support.serializer.JsonSerde;
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
  private String clicksTopicOut;

  @Bean
  public KStream<String, CartItem> cartItemStream(StreamsBuilder kStreamBuilder) {
    KStream<String, CartItem> stream =
        kStreamBuilder.stream(
            cartItemTopicIn,
            Consumed.with(Serdes.String(), CustomSerdes.getCartItemSerde(getSchemaProperties())));
    // stream.peek((k,v)->LOG.info("Key: " + k + ", Value: " + v ));
    return stream;
  }

  @Bean
  public GlobalKTable<String, Article> articles(StreamsBuilder kStreamBuilder) {
    GlobalKTable<String, Article> globalTable =
        kStreamBuilder.globalTable(
            articlesTopicIn,
            Consumed.with(Serdes.String(), CustomSerdes.getArticleSerde(getSchemaProperties())));
    return globalTable;
  }

  @Bean
  public KStream<String, CartItemPrice> cartItemPriceStream(
      KStream<String, CartItem> cartItemStream, GlobalKTable<String, Article> articles) {
    KStream<String, CartItemPrice> stream =
        cartItemStream.leftJoin(articles, (k, v) -> v.getArticleID(), cartItemPriceValueJoiner());
    return stream;
  }

  private ValueJoinerWithKey<String, CartItem, Article, CartItemPrice> cartItemPriceValueJoiner() {
    return (key, left, right) ->
        CartItemPrice.newBuilder()
            .setCartID(key)
            .setArticleID(left.getArticleID())
            .setChangeType(left.getChangeType())
            .setName(right.getName())
            .setCategory(right.getCategory())
            .setPrice(right.getPrice())
            .setDescription(right.getDescription())
            .build();
  }

  private Initializer<ShoppingCartAggregate> shoppingCartAggregateInitializer() {
    return () ->
        ShoppingCartAggregate.newBuilder()
            .setCartID("")
            .setCartPrice(0.0)
            .setCartItems(new ArrayList<>())
            .build();
  }

  private Aggregator<String, CartItemPrice, ShoppingCartAggregate> shoppingCartAggregator() {
    return (aggKey, value, aggregate) -> {
      if (value.getChangeType().equals(CartChangeType.ADDED)) {
        aggregate.setCartPrice(aggregate.getCartPrice() + value.getPrice());
      }
      if (value.getChangeType().equals(CartChangeType.REMOVED)) {
        aggregate.setCartPrice(aggregate.getCartPrice() - value.getPrice());
      }
      aggregate.getCartItems().add(value);
      return aggregate;
    };
  }

  @Bean
  public KTable<String, ShoppingCartAggregate> shoppingCartAggregateTable(
      KStream<String, CartItemPrice> cartItemPrice) {

    KTable<String, ShoppingCartAggregate> shoppingCartAggregate =
        cartItemPrice
            .groupByKey()
            .aggregate(
                shoppingCartAggregateInitializer(),
                shoppingCartAggregator(),
                Materialized.<String, ShoppingCartAggregate, KeyValueStore<Bytes, byte[]>>as(
                        "shop.carts.aggregate")
                    .withKeySerde(Serdes.String())
                    .withValueSerde(CustomSerdes.getCartItemAggregatSerde(getSchemaProperties())));
    shoppingCartAggregate.toStream()
            .peek((k,v)->LOG.info("Key: " + k + ", Value: " + v ));
    return shoppingCartAggregate;
  }

  private Properties getSchemaProperties() {
    Properties properties = new Properties();
    properties.put("schema.registry.url", schemaRegistryUrl);
    return properties;
  }
}
