package com.thinkport.producer.resource;

import digital.thinkport.avro.CartChangeType;
import digital.thinkport.avro.OrderPlaced;
import digital.thinkport.avro.ShoppingCart;
import digital.thinkport.avro.ShoppingCartAggregate;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.datafaker.Faker;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

@Service
@AllArgsConstructor
@Slf4j
public class ShoppingCartAggregateResource {

    private final KafkaTemplate<String, ShoppingCart> kafkaTemplateShoppingCart;
    private final KafkaTemplate<String, OrderPlaced> kafkaTemplateOrderPlaced;
    private static final String TOPIC_SHOPPING_CART = "shop.carts";
    private static final String TOPIC_ORDER_PLACED = "shop.orders.placed";
    private final Faker faker = new Faker();
    private final ConcurrentHashMap<Integer, ShoppingCartAggregate> shoppingCartsAggregateMap = new ConcurrentHashMap<>();
    private final AtomicBoolean initialized = new AtomicBoolean(false);

    @Scheduled(fixedRate = 1000)
    public void send() {
        if (!initialized.get()) {
            for (int i = 0; i < 20; i++) {
                ShoppingCart shoppingCart = newCart(CartChangeType.ADDED);
                kafkaTemplateShoppingCart.send(new ProducerRecord<>(TOPIC_SHOPPING_CART, shoppingCart.getCartID().toString(), shoppingCart));
                ShoppingCartAggregate shoppingCartAggregate = newCartAggregate(shoppingCart);
                shoppingCartsAggregateMap.put(i, shoppingCartAggregate);
            }
            initialized.set(true);
        }
        if (faker.number().numberBetween(1, 100) == 1) {
            //Order Placed
            int cartID = faker.number().numberBetween(0, 19);
            String orderID = faker.internet().uuid();

            ShoppingCartAggregate shoppingCartAggregate = shoppingCartsAggregateMap.get(cartID);

            OrderPlaced orderPlaced = OrderPlaced.newBuilder()
                    .setOrderID(orderID)
                    .setUserID(shoppingCartAggregate.getUserID())
                    .setCartID(shoppingCartAggregate.getCartID())
                    .build();

            kafkaTemplateOrderPlaced.send(new ProducerRecord<>(TOPIC_ORDER_PLACED, orderID, orderPlaced));
            shoppingCartsAggregateMap.remove(cartID);

            ShoppingCart newShoppingCart = newCart(CartChangeType.ADDED);
            ShoppingCartAggregate newShoppingCartAggregate = newCartAggregate(newShoppingCart);
            kafkaTemplateShoppingCart.send(new ProducerRecord<>(TOPIC_SHOPPING_CART, newShoppingCart.getCartID().toString(), newShoppingCart));
            shoppingCartsAggregateMap.put(cartID, newShoppingCartAggregate);
        }
        if (faker.number().numberBetween(1, 10) == 1) {

            System.out.println("Add article to order");
            int cartID = faker.number().numberBetween(0, 19);
            ShoppingCartAggregate shoppingCartAggregate = shoppingCartsAggregateMap.get(cartID);
            ShoppingCart shoppingCart = newCart(CartChangeType.ADDED, shoppingCartAggregate.getCartID().toString());
            kafkaTemplateShoppingCart.send(new ProducerRecord<>(TOPIC_SHOPPING_CART, shoppingCartAggregate.getCartID().toString(), shoppingCart));
            shoppingCartAggregate.getShoppingCarts().add(shoppingCart);
            shoppingCartsAggregateMap.put(cartID, shoppingCartAggregate);
            System.out.println("Add article to order Successfully");

        }
        if (faker.number().numberBetween(1, 20) == 1) {
            System.out.println("Remove article");
            int cartID = faker.number().numberBetween(0, 19);
            ShoppingCartAggregate shoppingCartAggregate = shoppingCartsAggregateMap.get(cartID);
            List<ShoppingCart> shoppingCarts = shoppingCartAggregate.getShoppingCarts();
            if (shoppingCarts.size() > 0) {
                int shopingCartId = faker.number().numberBetween(0, shoppingCarts.size() - 1);
                ShoppingCart deleteCart = shoppingCarts.get(shopingCartId);
                deleteCart.setChangeType(CartChangeType.REMOVED);
                shoppingCarts.set(shoppingCarts.size() - 1, deleteCart);
                kafkaTemplateShoppingCart.send(new ProducerRecord<>(TOPIC_SHOPPING_CART, shoppingCartAggregate.getCartID().toString(), deleteCart));
            }
        }
    }

    private ShoppingCartAggregate newCartAggregate(ShoppingCart shoppingCart) {
        List<ShoppingCart> shoppingCarts = new ArrayList<>();
        shoppingCarts.add(shoppingCart);
        return ShoppingCartAggregate.newBuilder()
                .setCartID(shoppingCart.getCartID())
                .setUserID(shoppingCart.getUserID())
                .setShoppingCarts(shoppingCarts)
                .build();
    }

    private ShoppingCart newCart(CartChangeType cartChangeType) {
        return ShoppingCart.newBuilder()
                .setCartID(faker.internet().uuid())
                .setUserID(String.valueOf(faker.number().numberBetween(1, 500)))
                .setArticleID(String.valueOf(faker.number().numberBetween(1, 500)))
                .setChangeType(cartChangeType)
                .build();
    }

    private ShoppingCart newCart(CartChangeType cartChangeType, String cartId) {
        return ShoppingCart.newBuilder()
                .setCartID(cartId)
                .setUserID(String.valueOf(faker.number().numberBetween(1, 500)))
                .setArticleID(String.valueOf(faker.number().numberBetween(1, 500)))
                .setChangeType(cartChangeType)
                .build();
    }

}
