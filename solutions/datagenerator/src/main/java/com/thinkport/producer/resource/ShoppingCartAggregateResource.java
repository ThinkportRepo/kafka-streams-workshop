package com.thinkport.producer.resource;

import digital.thinkport.avro.CartChangeType;
import digital.thinkport.avro.CartItem;
import digital.thinkport.avro.OrderPlaced;
import digital.thinkport.avro.ShoppingCartAggregateCreation;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.datafaker.Faker;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service
@AllArgsConstructor
@Slf4j
public class ShoppingCartAggregateResource {

  private static final String TOPIC_SHOPPING_CART = "shop.carts";
  private static final String TOPIC_ORDER_PLACED = "shop.orders.placed";
  private final KafkaTemplate<String, CartItem> kafkaTemplateShoppingCart;
  private final KafkaTemplate<String, OrderPlaced> kafkaTemplateOrderPlaced;
  private final Faker faker = new Faker();
  private final ConcurrentHashMap<Integer, ShoppingCartAggregateCreation>
      shoppingCartsAggregateMap = new ConcurrentHashMap<>();
  private final AtomicBoolean initialized = new AtomicBoolean(false);

  @Scheduled(fixedRate = 500)
  public void send() {
    if (!initialized.get()) {
      for (int i = 0; i < 20; i++) {
        CartItem shoppingCart = newCart(CartChangeType.ADDED);
        kafkaTemplateShoppingCart.send(
            new ProducerRecord<>(TOPIC_SHOPPING_CART, shoppingCart.getCartID(), shoppingCart));
        ShoppingCartAggregateCreation shoppingCartAggregate = newCartAggregate(shoppingCart);
        shoppingCartsAggregateMap.put(i, shoppingCartAggregate);
      }
      initialized.set(true);
    }
    if (faker.number().numberBetween(1, 100) == 1) {
      // Order Placed
      int cartID = faker.number().numberBetween(0, 19);
      String orderID = faker.internet().uuid();

      ShoppingCartAggregateCreation shoppingCartAggregate = shoppingCartsAggregateMap.get(cartID);

      OrderPlaced orderPlaced =
          OrderPlaced.newBuilder()
              .setOrderID(orderID)
              .setUserID(String.valueOf(faker.number().numberBetween(0, 500)))
              .setCartID(shoppingCartAggregate.getCartID())
              .build();

      kafkaTemplateOrderPlaced.send(
          new ProducerRecord<>(TOPIC_ORDER_PLACED, shoppingCartAggregate.getCartID(), orderPlaced));
      shoppingCartsAggregateMap.remove(cartID);

      CartItem newShoppingCart = newCart(CartChangeType.ADDED);
      ShoppingCartAggregateCreation newShoppingCartAggregate = newCartAggregate(newShoppingCart);
      kafkaTemplateShoppingCart.send(
          new ProducerRecord<>(TOPIC_SHOPPING_CART, newShoppingCart.getCartID(), newShoppingCart));
      shoppingCartsAggregateMap.put(cartID, newShoppingCartAggregate);
      System.out.println("Order Placed");
    }
    if (faker.number().numberBetween(1, 5) == 1) {

      System.out.println("Add article to order");
      int cartID = faker.number().numberBetween(0, 19);
      ShoppingCartAggregateCreation shoppingCartAggregate = shoppingCartsAggregateMap.get(cartID);
      CartItem shoppingCart = newCart(CartChangeType.ADDED, shoppingCartAggregate.getCartID());
      kafkaTemplateShoppingCart.send(
          new ProducerRecord<>(
              TOPIC_SHOPPING_CART, shoppingCartAggregate.getCartID(), shoppingCart));
      shoppingCartAggregate.getCartItem().add(shoppingCart);
      shoppingCartsAggregateMap.put(cartID, shoppingCartAggregate);
      System.out.println("Add article to order Successfully");
    }
    if (faker.number().numberBetween(1, 20) == 1) {
      System.out.println("Remove article");
      int cartID = faker.number().numberBetween(0, 19);
      ShoppingCartAggregateCreation shoppingCartAggregate = shoppingCartsAggregateMap.get(cartID);
      List<CartItem> shoppingCarts = shoppingCartAggregate.getCartItem();
      if (shoppingCarts.size() > 0) {
        int shopingCartId = faker.number().numberBetween(0, shoppingCarts.size() - 1);
        CartItem deleteCart = shoppingCarts.get(shopingCartId);
        deleteCart.setChangeType(CartChangeType.REMOVED);
        shoppingCarts.set(shoppingCarts.size() - 1, deleteCart);
        kafkaTemplateShoppingCart.send(
            new ProducerRecord<>(
                TOPIC_SHOPPING_CART, shoppingCartAggregate.getCartID(), deleteCart));
      }
    }
  }

  private ShoppingCartAggregateCreation newCartAggregate(CartItem shoppingCart) {
    List<CartItem> shoppingCarts = new ArrayList<>();
    shoppingCarts.add(shoppingCart);
    return ShoppingCartAggregateCreation.newBuilder()
        .setCartID(shoppingCart.getCartID())
        .setCartItem(shoppingCarts)
        .build();
  }

  private CartItem newCart(CartChangeType cartChangeType) {
    return CartItem.newBuilder()
        .setCartID(faker.internet().uuid())
        .setArticleID(String.valueOf(faker.number().numberBetween(1, 500)))
        .setChangeType(cartChangeType)
        .build();
  }

  private CartItem newCart(CartChangeType cartChangeType, String cartId) {
    return CartItem.newBuilder()
        .setCartID(cartId)
        .setArticleID(String.valueOf(faker.number().numberBetween(1, 500)))
        .setChangeType(cartChangeType)
        .build();
  }
}
