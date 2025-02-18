package com.thinkport.producer.processor;

import com.google.common.util.concurrent.AtomicDouble;
import digital.thinkport.avro.*;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

import lombok.AllArgsConstructor;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.SessionStore;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;

@SpringBootApplication
@AllArgsConstructor
public class ShoppingCartAggregateProccessor {
  private static final String SHOPPING_CART_TOPIC = "shop.carts";
  private static final String ORDER_PLACED_TOPIC = "shop.orders.placed";
  private static final String ORDER_SUMMARY_TOPIC = "order_summary";

  @Bean
  public StreamsConfig streamsConfig() {
    return new StreamsConfig(
        Map.of(
            StreamsConfig.APPLICATION_ID_CONFIG, "order-aggregation-app",
            StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, "org.apache.kafka.common.serialization.Serdes$StringSerde"));


  }

  @Bean
  public StreamsBuilder streamsBuilder() {
    return new StreamsBuilder();
  }

  @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
  public KafkaStreams kStreamsConfigs(StreamsBuilder builder) {
    /*
       System.out.println("Streams startet");
       Serde<OrderPlaced> orderSerde = new SpecificAvroSerde<>();
       Serde<ShoppingCart> cartSerde = new SpecificAvroSerde<>();
       Serde<OrderSummary> orderSummarySerde = new SpecificAvroSerde<>();
       Serde<Double> doubleSerde = Serdes.Double();

       orderSerde.configure(Map.of("schema.registry.url", "http://localhost:8081"), false);
       cartSerde.configure(Map.of("schema.registry.url", "http://localhost:8081"), false);
       orderSummarySerde.configure(Map.of("schema.registry.url", "http://localhost:8081"), false);

       KStream<String, OrderPlaced> orders = builder.stream("shop.orders.placed", Consumed.with(Serdes.String(), orderSerde));
       KTable<String, Double> cartTotals = builder.stream("shop.carts", Consumed.with(Serdes.String(), cartSerde))
               .groupBy((key, cart) -> cart.getCartID().toString(), Grouped.with(Serdes.String(), cartSerde))
               .aggregate(
                       () -> 0.0,
                       (cartID, cart, total) -> cart.getChangeType().toString().equals("ADDED") ? total + 1.0 : total - 1.0,
                       Materialized.with(Serdes.String(), doubleSerde)
               );

       cartTotals.toStream().foreach((key, value) ->
               System.out.println("KTable Entry - cartID: " + key + ", totalValue: " + value)
       );


       KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfig());

    */
    Serde<CartItem> cartChangeSerde = new SpecificAvroSerde<>();
    cartChangeSerde.configure(Map.of("schema.registry.url", "http://localhost:8081"), false);

    Serde<Article> articleSerde = new SpecificAvroSerde<>();
    articleSerde.configure(Map.of("schema.registry.url", "http://localhost:8081"), false);

    Serde<ShoppingCartAggregate> shoppingCartAggregateSerde = new SpecificAvroSerde<>();
    shoppingCartAggregateSerde.configure(
        Map.of("schema.registry.url", "http://localhost:8081"), false);

    Serde<OrderPlaced> orderPlacedSerde = new SpecificAvroSerde<>();
    orderPlacedSerde.configure(Map.of("schema.registry.url", "http://localhost:8081"), false);

    Serde<CartItemPrice> cartItemPriceSerde = new SpecificAvroSerde<>();
    cartItemPriceSerde.configure(Map.of("schema.registry.url", "http://localhost:8081"), false);

      Serde<UserTotalOrder> userTotalOrderSerde = new SpecificAvroSerde<>();
      userTotalOrderSerde.configure(Map.of("schema.registry.url", "http://localhost:8081"), false);


      Serde<ReducedCartOrder> reducedCartOrderSerde = new SpecificAvroSerde<>();
      reducedCartOrderSerde.configure(Map.of("schema.registry.url", "http://localhost:8081"), false);

    GlobalKTable<String, Article> articles =
        builder.globalTable("shop.articles", Consumed.with(Serdes.String(), articleSerde));

    KStream<String, CartItem> cartStream =
        builder.stream("shop.carts", Consumed.with(Serdes.String(), cartChangeSerde));

    KStream<String, OrderPlaced> orderStream =
        builder.stream("shop.orders.placed", Consumed.with(Serdes.String(), orderPlacedSerde));

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

    KStream<String, CartItemPrice> cartItemPrice =
        cartStream.leftJoin(
            articles, (k, v) -> v.getArticleID().toString(), cartItemPriceValueJoiner);

    // cartItemPrice.foreach((k,v) -> System.out.println(v.toString()));

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

    KTable<String, ShoppingCartAggregate> shoppingCartAggregate =
        cartItemPrice
            .groupByKey()
            .aggregate(
                shoppingCartAggregateInitializer,
                shoppingCartAggregator,
                Materialized.<String, ShoppingCartAggregate, KeyValueStore<Bytes, byte[]>>as(
                        "shop.carts.aggregate")
                    .withKeySerde(Serdes.String())
                    .withValueSerde(shoppingCartAggregateSerde));

    // shoppingCartAggregate.toStream().foreach((k, v) -> System.out.println(v.toString()));

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

    KStream<String, TotalCartOrder> totalCartOrder =
        orderStream.leftJoin(shoppingCartAggregate, totalCartOrderValueJoiner);


      Aggregator<String, ReducedCartOrder, UserTotalOrder> userTotalAggregator =
              (aggKey, value, aggregate) -> {
                    aggregate.setUserID(value.getUserID());
                    aggregate.setOrderTotal(aggregate.getOrderTotal() + value.getOrderTotal());
                    if(aggregate.getMostExpensiveCartItem().getPrice() < value.getMostExpensiveCartItem().getPrice()){
                        aggregate.setMostExpensiveCartItem(value.getMostExpensiveCartItem());
                    }
                  return aggregate;
              };



      Initializer<UserTotalOrder> userTotalOrderInitializer =
              () -> {
                 MostExpensiveCartItem mostExpensiveCartItem =  MostExpensiveCartItem.newBuilder().setArticleID("").setPrice(0.0).build();
                  return UserTotalOrder.newBuilder()
                          .setUserID("")
                          .setOrderTotal(0.0)
                          .setMostExpensiveCartItem(mostExpensiveCartItem)
                          .build();
              };

      Merger<String, UserTotalOrder> merger = (k, leftAgg, rightAgg) -> {
          leftAgg.setUserID(rightAgg.getUserID());
          leftAgg.setUserID(rightAgg.getUserID());
          leftAgg.setUserID(rightAgg.getUserID());
          leftAgg.setOrderTotal(leftAgg.getOrderTotal() + rightAgg.getOrderTotal());
          if(leftAgg.getMostExpensiveCartItem().getPrice() < rightAgg.getMostExpensiveCartItem().getPrice()){
              leftAgg.setMostExpensiveCartItem(rightAgg.getMostExpensiveCartItem());
          }
          return leftAgg;
      };

      KStream<Windowed<String>, UserTotalOrder> totalOrderKStream= totalCartOrder.map((k, v) -> {
        Map<CharSequence, Integer> map = new HashMap();
        Map<CharSequence, CartItemPrice> articleMap= new HashMap();

        v.getCartItems().stream().forEach( a -> {
            articleMap.put(a.getArticleID(), a);
            int currentOccurrence = map.containsKey(a.getArticleID()) ? map.get(a.getArticleID()) : 0;
            if(a.getChangeType().equals(CartChangeType.ADDED)){
                currentOccurrence += 1;
            }else {
                currentOccurrence -= 1;
            }
            map.put(a.getArticleID(), currentOccurrence);
        } );

        AtomicDouble highestPrice = new AtomicDouble(0);
        AtomicReference<CharSequence> highestPriceName = new AtomicReference<>();

        map.forEach((a,b) -> {
            if (b > 0 && articleMap.get(a).getPrice() > highestPrice.get() ){
                highestPrice.set(articleMap.get(a).getPrice());
                highestPriceName.set(articleMap.get(a).getArticleID());
            }
        });

            MostExpensiveCartItem mostExpensiceCartItem = MostExpensiveCartItem.newBuilder()
                .setArticleID(articleMap.get(highestPriceName).getArticleID())
                .setPrice(highestPrice.get())
                .build();

        ReducedCartOrder reducedCartOrder = ReducedCartOrder.newBuilder()
                .setOrderID(v.getOrderID())
                .setUserID(v.getUserID())
                .setCartID(v.getCartID())
                .setOrderTotal(v.getOrderTotal())
                .setMostExpensiveCartItem(mostExpensiceCartItem)
                .build();

        return new KeyValue<>(v.getUserID().toString(), reducedCartOrder);
    }).groupByKey(Grouped.<String, ReducedCartOrder>as("shop.order.reduced").withKeySerde(Serdes.String()).withValueSerde(reducedCartOrderSerde))
              .windowedBy(SessionWindows.ofInactivityGapAndGrace(Duration.ofSeconds(5), Duration.ofSeconds(2)))
              .aggregate(
                      userTotalOrderInitializer,
                      userTotalAggregator,
                      merger,
                      Materialized.<String, UserTotalOrder, SessionStore<Bytes, byte[]>>as(
                                      "shop.user.total")
                              .withKeySerde(Serdes.String())
                              .withValueSerde(userTotalOrderSerde)
              )
              .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded().shutDownWhenFull()))
              .filter((k,v) -> v.getOrderTotal() > 100 )
              .toStream();

    totalOrderKStream.foreach((k, v) -> System.out.println(v.toString()));

    KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfig());
    streams.start();

    return streams;
  }
}
