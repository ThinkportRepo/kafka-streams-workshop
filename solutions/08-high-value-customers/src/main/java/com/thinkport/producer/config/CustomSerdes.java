package com.thinkport.streams.config;

import digital.thinkport.avro.*;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.avro.specific.SpecificRecord;

public class CustomSerdes {
  public static SpecificAvroSerde<Article> getArticleSerde(Properties properties) {
    return getGenericSerde(properties);
  }

  public static SpecificAvroSerde<CartItem> getCartItemSerde(Properties properties) {
    return getGenericSerde(properties);
  }

  public static SpecificAvroSerde<OrderPlaced> getOrderPlacedSerde(Properties properties) {
    return getGenericSerde(properties);
  }

  public static SpecificAvroSerde<ShoppingCartAggregate> getShoppingCartAggregateSerde(
      Properties properties) {
    return getGenericSerde(properties);
  }

  public static SpecificAvroSerde<ReducedCartOrder> getReducedCartOrderSerde(
      Properties properties) {
    return getGenericSerde(properties);
  }

  public static SpecificAvroSerde<UserTotalOrder> getUserTotalOrderSerde(Properties properties) {
    return getGenericSerde(properties);
  }

  private static <T extends SpecificRecord> SpecificAvroSerde<T> getGenericSerde(
      Properties properties) {
    final Map<String, String> genericSerdeConfig = new HashMap<>();
    genericSerdeConfig.put("schema.registry.url", properties.getProperty("schema.registry.url"));
    final SpecificAvroSerde<T> genericSerde = new SpecificAvroSerde<>();
    genericSerde.configure(genericSerdeConfig, false); // `false` for record values
    return genericSerde;
  }
}
