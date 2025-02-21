package com.thinkport.producer.topology;

import com.google.common.util.concurrent.AtomicDouble;
import com.thinkport.streams.config.CustomSerdes;
import digital.thinkport.avro.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.SessionStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class MainTopology {
  private final ValueJoinerWithKey<String, CartItem, Article, CartItemPrice> cartItemPriceValueJoiner =
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
    private final Initializer<ShoppingCartAggregate> shoppingCartAggregateInitializer =
             () -> ShoppingCartAggregate.newBuilder()
                        .setCartID("")
                        .setCartPrice(0.0)
                        .setCartItems(new ArrayList<>())
                        .build();
    private final Aggregator<String, CartItemPrice, ShoppingCartAggregate> shoppingCartAggregator =
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
    private final KeyValueMapper<String,TotalCartOrder,KeyValue<String, ReducedCartOrder>> reduceTotalCartOrder =
       (k, v) -> {
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
        if(highestPriceName.get() == null){
            return new KeyValue<>(v.getUserID(), null);
        }
        MostExpensiveCartItem mostExpensiceCartItem = MostExpensiveCartItem.newBuilder()
                .setArticleID(articleMap.get(highestPriceName.get()).getArticleID())
                .setPrice(highestPrice.get())
                .build();

           if(v.getUserID() == null){
               return new KeyValue<>(v.getUserID(), null);
           }

        ReducedCartOrder reducedCartOrder = ReducedCartOrder.newBuilder()
                .setOrderID(v.getOrderID())
                .setUserID(v.getUserID())
                .setCartID(v.getCartID())
                .setOrderTotal(v.getOrderTotal())
                .setMostExpensiveCartItem(mostExpensiceCartItem)
                .build();

        return new KeyValue<>(v.getUserID(), reducedCartOrder);
    };
    private final ValueJoinerWithKey<String, OrderPlaced, ShoppingCartAggregate, TotalCartOrder> totalCartOrderValueJoiner =
        (key, left, right) -> {
            return TotalCartOrder.newBuilder()
                    .setCartID(key)
                    .setOrderID(left.getOrderID())
                    .setUserID(left.getUserID())
                    .setOrderTotal(right.getCartPrice())
                    .setCartItems(right.getCartItems())
                    .build();
        };
    private final Aggregator<String, ReducedCartOrder, UserTotalOrder> userTotalAggregator =
            (aggKey, value, aggregate) -> {
                if(value != null){
                    aggregate.setUserID(value.getUserID());
                    aggregate.setOrderTotal(aggregate.getOrderTotal() + value.getOrderTotal());
                    if(aggregate.getMostExpensiveCartItem().getPrice() < value.getMostExpensiveCartItem().getPrice()){
                        aggregate.setMostExpensiveCartItem(value.getMostExpensiveCartItem());
                    }
                }
                return aggregate;
            };
    private final Initializer<UserTotalOrder> userTotalOrderInitializer =
            () -> {
                MostExpensiveCartItem mostExpensiveCartItem =  MostExpensiveCartItem.newBuilder().setArticleID("").setPrice(0.0).build();
                return UserTotalOrder.newBuilder()
                        .setUserID("")
                        .setOrderTotal(0.0)
                        .setMostExpensiveCartItem(mostExpensiveCartItem)
                        .build();
            };
    private final Merger<String, UserTotalOrder> merger = (k, leftAgg, rightAgg) -> {
        leftAgg.setUserID(rightAgg.getUserID());
        leftAgg.setUserID(rightAgg.getUserID());
        leftAgg.setUserID(rightAgg.getUserID());
        leftAgg.setOrderTotal(leftAgg.getOrderTotal() + rightAgg.getOrderTotal());
        if(leftAgg.getMostExpensiveCartItem().getPrice() < rightAgg.getMostExpensiveCartItem().getPrice()){
            leftAgg.setMostExpensiveCartItem(rightAgg.getMostExpensiveCartItem());
        }
        return leftAgg;
    };
    @Value("${spring.kafka.properties.schema.registry.url}")
    private String schemaRegistryUrl;

  @Autowired
  public void topology(StreamsBuilder builder) {
      //Init all required input topic streams and tables
    GlobalKTable<String, Article> articles =
        builder.globalTable("shop.articles", Consumed.with(Serdes.String(), CustomSerdes.getArticleSerde(getSchemaProperties())));
    KStream<String, CartItem> cartStream =
        builder.stream("shop.carts", Consumed.with(Serdes.String(), CustomSerdes.getCartItemSerde(getSchemaProperties())));

    KStream<String, OrderPlaced> orderStream =
        builder.stream("shop.orders.placed", Consumed.with(Serdes.String(), CustomSerdes.getOrderPlacedSerde(getSchemaProperties())));

    //Create aggregated Carts from Cart items and Article infos
    KStream<String, CartItemPrice> cartItemPrice =
        cartStream.leftJoin(
            articles, (k, v) -> v.getArticleID(), cartItemPriceValueJoiner);

    KTable<String, ShoppingCartAggregate> shoppingCartAggregate =
        cartItemPrice
            .groupByKey()
            .aggregate(
                shoppingCartAggregateInitializer,
                shoppingCartAggregator,
                Materialized.<String, ShoppingCartAggregate, KeyValueStore<Bytes, byte[]>>as(
                        "shop.carts.aggregate")
                    .withKeySerde(Serdes.String())
                    .withValueSerde(CustomSerdes.getShoppingCartAggregateSerde(getSchemaProperties())));

      //Get Shopping Cart, if an order was placed
    KStream<String, TotalCartOrder> totalCartOrder =
        orderStream.leftJoin(shoppingCartAggregate, totalCartOrderValueJoiner);

      //Aggregate Order per User
      KStream<Windowed<String>, UserTotalOrder> highValueUserOrders = totalCartOrder
              .map(reduceTotalCartOrder)
              .groupByKey(Grouped.<String, ReducedCartOrder>as("shop.order.reduced").withKeySerde(Serdes.String()).withValueSerde(CustomSerdes.getReducedCartOrderSerde(getSchemaProperties())))
              .windowedBy(SessionWindows.ofInactivityGapAndGrace(Duration.ofSeconds(30), Duration.ofSeconds(2)))
              .aggregate(
                      userTotalOrderInitializer,
                      userTotalAggregator,
                      merger,
                      Materialized.<String, UserTotalOrder, SessionStore<Bytes, byte[]>>as(
                                      "shop.user.total")
                              .withKeySerde(Serdes.String())
                              .withValueSerde(CustomSerdes.getUserTotalOrderSerde(getSchemaProperties()))
              )
              .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded().shutDownWhenFull()))
              .filter((k,v) -> v.getOrderTotal() > 20 )
              .toStream();

      //Return HVC
      highValueUserOrders
              .selectKey((k,v)->k.key())
              .to("shop.hvc",Produced.with(Serdes.String(),CustomSerdes.getUserTotalOrderSerde(getSchemaProperties())));
  }

    private Properties getSchemaProperties() {
        Properties properties = new Properties();
        properties.put("schema.registry.url", schemaRegistryUrl);
        return properties;
    }
}
