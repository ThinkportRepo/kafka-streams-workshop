package com.thinkport.streams.topology;

import com.google.common.util.concurrent.AtomicDouble;
import com.thinkport.streams.config.CustomSerdes;
import digital.thinkport.avro.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.SessionStore;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

@Component
@Slf4j
public class StreamTopology {
  @Value("${spring.kafka.properties.schema.registry.url}")
  private String schemaRegistryUrl;

  @Value("${kafka-topics.article-in}")
  private String articleTopic;

  @Value("${kafka-topics.cart-item-in}")
  private String cartItem;

  @Value("${kafka-topics.order-placed-in}")
  private String orderPlacedTopic;

  @Value("${kafka-topics.order-reduced-out}")
  private String orderReducedOut;

  @Value("${kafka-topics.shop-user-total}")
  private String shopUserTotal;

  @Bean
  GlobalKTable<String, Article> articleGlobalKTable(StreamsBuilder kStreamBuilder) {
    return kStreamBuilder.globalTable(
        articleTopic,
        Consumed.with(Serdes.String(), CustomSerdes.getArticleSerde(getSchemaProperties())));
  }

  @Bean
  public KStream<String, CartItem> cartStream(StreamsBuilder kStreamBuilder) {
    return kStreamBuilder.stream(
        cartItem,
        Consumed.with(Serdes.String(), CustomSerdes.getCartItemSerde(getSchemaProperties())));
  }

  @Bean
  public KStream<String, OrderPlaced> orderPlacedStream(StreamsBuilder kStreamBuilder) {
    return kStreamBuilder.stream(
        orderPlacedTopic,
        Consumed.with(Serdes.String(), CustomSerdes.getOrderPlacedSerde(getSchemaProperties())));
  }

  @Bean
  public KStream<String, CartItemPrice> cartItemPrice(
      KStream<String, CartItem> cartStream, GlobalKTable<String, Article> articles) {
    ValueJoinerWithKey<String, CartItem, Article, CartItemPrice> cartItemPriceValueJoiner =
        (key, left, right) -> {
          return CartItemPrice.newBuilder()
              .setCartID(key)
              .setArticleID(left.getArticleID())
              .setChangeType(left.getChangeType())
              .setName(right.getName())
              .setCategory(right.getCategory())
              .setPrice(right.getPrice())
              .setDescription(right.getDescription())
              .build();
        };
    return cartStream.leftJoin(
        articles, (k, v) -> v.getArticleID().toString(), cartItemPriceValueJoiner);
  }

  @Bean
  public KTable<String, ShoppingCartAggregate> shoppingCartAggregate(
      KStream<String, CartItemPrice> cartItemPrice) {
    Aggregator<String, CartItemPrice, ShoppingCartAggregate> shoppingCartAggregator =
        (aggKey, value, aggregate) -> {
          if (value.getChangeType().equals(CartChangeType.ADDED)) {
            aggregate.setCartPrice(aggregate.getCartPrice() + value.getPrice());
          }
          if (value.getChangeType().equals(CartChangeType.REMOVED)) {
            aggregate.setCartPrice(aggregate.getCartPrice() - value.getPrice());
          }
          aggregate.getCartItems().add(value);
          return aggregate;
        };

    Initializer<ShoppingCartAggregate> shoppingCartAggregateInitializer =
        () -> {
          return ShoppingCartAggregate.newBuilder()
              .setCartID("")
              .setCartPrice(0.0)
              .setCartItems(new ArrayList<>())
              .build();
        };

    return cartItemPrice
        .groupByKey()
        .aggregate(
            shoppingCartAggregateInitializer,
            shoppingCartAggregator,
            Materialized.<String, ShoppingCartAggregate, KeyValueStore<Bytes, byte[]>>as(
                    "shop.carts.aggregate")
                .withKeySerde(Serdes.String())
                .withValueSerde(CustomSerdes.getShoppingCartAggregateSerde(getSchemaProperties())));
  }

  @Bean
  public KStream<String, TotalCartOrder> totalCartOrderStream(
      KStream<String, OrderPlaced> orderPlacedStream,
      KTable<String, ShoppingCartAggregate> shoppingCartAggregate) {
    ValueJoinerWithKey<String, OrderPlaced, ShoppingCartAggregate, TotalCartOrder>
        totalCartOrderValueJoiner =
            (key, left, right) -> {
              return TotalCartOrder.newBuilder()
                  .setCartID(key)
                  .setOrderID(left.getOrderID())
                  .setUserID(left.getUserID())
                  .setOrderTotal(right.getCartPrice())
                  .setCartItems(right.getCartItems())
                  .build();
            };
    return orderPlacedStream.leftJoin(shoppingCartAggregate, totalCartOrderValueJoiner);
  }

  @Bean
  public KStream<Windowed<String>, UserTotalOrder> highValueCustomer(
      KStream<String, TotalCartOrder> totalCartOrderStream) {
    Aggregator<String, ReducedCartOrder, UserTotalOrder> userTotalAggregator =
        (aggKey, value, aggregate) -> {
          aggregate.setUserID(value.getUserID());
          aggregate.setOrderTotal(aggregate.getOrderTotal() + value.getOrderTotal());
          if (aggregate.getMostExpensiveCartItem().getPrice()
              < value.getMostExpensiveCartItem().getPrice()) {
            aggregate.setMostExpensiveCartItem(value.getMostExpensiveCartItem());
          }
          return aggregate;
        };

    Initializer<UserTotalOrder> userTotalOrderInitializer =
        () -> {
          MostExpensiveCartItem mostExpensiveCartItem =
              MostExpensiveCartItem.newBuilder().setArticleID("").setPrice(0.0).build();
          return UserTotalOrder.newBuilder()
              .setUserID("")
              .setOrderTotal(0.0)
              .setMostExpensiveCartItem(mostExpensiveCartItem)
              .build();
        };

    Merger<String, UserTotalOrder> merger =
        (k, leftAgg, rightAgg) -> {
          leftAgg.setUserID(rightAgg.getUserID());
          leftAgg.setUserID(rightAgg.getUserID());
          leftAgg.setUserID(rightAgg.getUserID());
          leftAgg.setOrderTotal(leftAgg.getOrderTotal() + rightAgg.getOrderTotal());
          if (leftAgg.getMostExpensiveCartItem().getPrice()
              < rightAgg.getMostExpensiveCartItem().getPrice()) {
            leftAgg.setMostExpensiveCartItem(rightAgg.getMostExpensiveCartItem());
          }
          return leftAgg;
        };

    return totalCartOrderStream
        .map(
            (k, v) -> {
              Map<CharSequence, Integer> map = new HashMap();
              Map<CharSequence, CartItemPrice> articleMap = new HashMap();

              v.getCartItems().stream()
                  .forEach(
                      a -> {
                        articleMap.put(a.getArticleID(), a);
                        int currentOccurrence =
                            map.containsKey(a.getArticleID()) ? map.get(a.getArticleID()) : 0;
                        if (a.getChangeType().equals(CartChangeType.ADDED)) {
                          currentOccurrence += 1;
                        } else {
                          currentOccurrence -= 1;
                        }
                        map.put(a.getArticleID(), currentOccurrence);
                      });

              AtomicDouble highestPrice = new AtomicDouble(0);
              AtomicReference<CharSequence> highestPriceName = new AtomicReference<>();

              map.forEach(
                  (a, b) -> {
                    if (b > 0 && articleMap.get(a).getPrice() > highestPrice.get()) {
                      highestPrice.set(articleMap.get(a).getPrice());
                      highestPriceName.set(articleMap.get(a).getArticleID());
                    }
                  });

              MostExpensiveCartItem mostExpensiceCartItem =
                  MostExpensiveCartItem.newBuilder()
                      .setArticleID(articleMap.get(highestPriceName).getArticleID())
                      .setPrice(highestPrice.get())
                      .build();

              ReducedCartOrder reducedCartOrder =
                  ReducedCartOrder.newBuilder()
                      .setOrderID(v.getOrderID())
                      .setUserID(v.getUserID())
                      .setCartID(v.getCartID())
                      .setOrderTotal(v.getOrderTotal())
                      .setMostExpensiveCartItem(mostExpensiceCartItem)
                      .build();

              return new KeyValue<>(v.getUserID().toString(), reducedCartOrder);
            })
        .groupByKey(
            Grouped.<String, ReducedCartOrder>as(orderReducedOut)
                .withKeySerde(Serdes.String())
                .withValueSerde(CustomSerdes.getReducedCartOrderSerde(getSchemaProperties())))
        .windowedBy(
            SessionWindows.ofInactivityGapAndGrace(Duration.ofSeconds(5), Duration.ofSeconds(2)))
        .aggregate(
            userTotalOrderInitializer,
            userTotalAggregator,
            merger,
            Materialized.<String, UserTotalOrder, SessionStore<Bytes, byte[]>>as(shopUserTotal)
                .withKeySerde(Serdes.String())
                .withValueSerde(CustomSerdes.getUserTotalOrderSerde(getSchemaProperties())))
        .suppress(
            Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded().shutDownWhenFull()))
        .filter((k, v) -> v.getOrderTotal() > 100)
        .toStream();
  }

  private Properties getSchemaProperties() {
    Properties properties = new Properties();
    properties.put("schema.registry.url", schemaRegistryUrl);
    return properties;
  }
}
